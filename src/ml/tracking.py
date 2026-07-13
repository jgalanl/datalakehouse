import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import reduce
from typing import Callable

os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")

import mlflow
import mlflow.spark
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, lit, mean as spark_mean, when
from pyspark.sql.types import DoubleType

from common.spark_factory import get_spark
from common import nessie_manager as nessie

MLFLOW_TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]

# Raíz de artefactos: una carpeta por experimento en vez de los ids numéricos de MLflow.
MLFLOW_ARTIFACT_ROOT = "s3://mlflow-artifacts"

# Fijamos el entorno del modelo; la auto-inferencia de MLflow no funciona en spark-master.
MODEL_PIP_REQUIREMENTS = ["pyspark==3.5.8", "mlflow==2.22.1"]


def progress(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def get_ml_spark(app_name: str) -> SparkSession:
    spark = get_spark(app_name=app_name)
    nessie.set_spark_branch(spark, "main")
    return spark


def setup_mlflow(experiment_name: str) -> None:
    """Activa el experimento. Lo crea (con un artifact_location con nombre) la primera
    vez, y lo restaura si alguien lo había borrado de forma lógica."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        artifact_location = f"{MLFLOW_ARTIFACT_ROOT}/{experiment_name}"
        client.create_experiment(experiment_name, artifact_location=artifact_location)
        progress(
            f"Experimento MLflow '{experiment_name}' creado en {artifact_location}."
        )
    elif exp.lifecycle_stage == "deleted":
        client.restore_experiment(exp.experiment_id)
        progress(f"Restaurado el experimento MLflow borrado lógicamente '{experiment_name}'.")
    mlflow.set_experiment(experiment_name)


# --- Evaluación ---

REGRESSION_METRICS: tuple[str, ...] = ("rmse", "mae", "r2")

# Cuántos fits de estimador corren a la vez por fold: uno por JVM de worker.
CV_PARALLELISM: int = 2

# Filas de common_test que puntuamos al medir la latencia de inferencia del @champion.
INFERENCE_LATENCY_SAMPLE_ROWS: int = 1000

# Tope de versiones que se conservan. Las que tienen alias (@champion/@challenger) quedan exentas.
REGISTRY_MAX_VERSIONS: int = 20
REGISTRY_PROTECTED_ALIASES: tuple[str, ...] = ("champion", "challenger")


def evaluate_predictions(
    predictions_df: DataFrame, label_col: str,
) -> dict[str, float]:
    """{rmse, mae, r2} para un df con `label_col` + `prediction`. Conviene cachear antes:
    cada métrica hace su propia pasada completa."""
    return {
        m: float(RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName=m,
        ).evaluate(predictions_df))
        for m in REGRESSION_METRICS
    }


def evaluate_regression(
    model, df: DataFrame, label_col: str,
) -> dict[str, float]:
    """Puntúa `model` sobre `df` y devuelve {rmse, mae, r2}."""
    predictions = model.transform(df).select(label_col, "prediction").cache()
    try:
        return evaluate_predictions(predictions, label_col)
    finally:
        predictions.unpersist()


def log_split_metrics(metrics: dict[str, float], split: str) -> None:
    for name, value in metrics.items():
        mlflow.log_metric(f"{split}_{name}", value)


# (Dataset de mlflow, context); context es la etiqueta bajo "Datasets used" en la UI.
DatasetInput = tuple["mlflow.data.dataset.Dataset", str]


def log_input_datasets(input_datasets: list[DatasetInput] | None) -> None:
    """Asocia cada (dataset, context) al run activo; no hace nada si es None."""
    for dataset, context in input_datasets or []:
        mlflow.log_input(dataset, context=context)


# --- Utilidades de entrenamiento ---

def train_and_log_run(
    *,
    splits: dict[str, DataFrame],
    label_col: str,
    pipeline: Pipeline,
    run_name: str,
    tags: dict,
    extra_params: dict | None = None,
    input_datasets: list[DatasetInput] | None = None,
) -> tuple[str, dict[str, dict[str, float]], object]:
    """Entrena con splits['train'], puntúa cada split y lo registra todo en MLflow.
    Devuelve (run_id, métricas por split, modelo entrenado) para que quien llama lo reutilice."""
    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        mlflow.set_tags(tags)
        log_input_datasets(input_datasets)
        if extra_params:
            mlflow.log_params(extra_params)

        progress(f"  [{run_name}] entrenando ({len(pipeline.getStages())} etapas)...")
        t0 = time.perf_counter()
        model = pipeline.fit(splits["train"])
        progress(f"  [{run_name}] entrenamiento hecho en {time.perf_counter() - t0:.1f}s")

        all_metrics: dict[str, dict[str, float]] = {}
        for split_name, split_df in splits.items():
            t1 = time.perf_counter()
            metrics = evaluate_regression(model, split_df, label_col)
            log_split_metrics(metrics, split_name)
            all_metrics[split_name] = metrics
            progress(
                f"  [{run_name}] {split_name}: rmse={metrics['rmse']:.4f} "
                f"mae={metrics['mae']:.4f} r2={metrics['r2']:.4f} "
                f"({time.perf_counter() - t1:.1f}s)"
            )

        progress(f"  [{run_name}] registrando modelo...")
        t2 = time.perf_counter()
        mlflow.spark.log_model(
            model, artifact_path="model", pip_requirements=MODEL_PIP_REQUIREMENTS,
        )
        progress(f"  [{run_name}] modelo registrado en {time.perf_counter() - t2:.1f}s")
    return run_id, all_metrics, model


def tune_with_cv(
    *,
    algorithm: str,
    train_df: DataFrame,
    eval_splits: dict[str, DataFrame],
    label_col: str,
    pipeline_factory: Callable[[str, dict], Pipeline],
    param_list: list[dict],
    num_folds: int,
    run_name: str,
    tags: dict,
    extra_params: dict,
    input_datasets: list[DatasetInput] | None = None,
) -> tuple[str, dict, dict[str, dict[str, float]], object]:
    """CV de k folds sobre `param_list` para un algoritmo.

    Entrena las etapas de features compartidas una vez por fold y cachea los vectores
    transformados, y luego solo reentrena el estimador por cada punto del grid. Así evita
    tanto la fuga entre folds como el preprocesado redundante que CrossValidator hace en
    cada punto. bestModel es el pipeline completo reentrenado sobre todo train_df con los
    parámetros ganadores. Devuelve (run_id, best_params, {split: {métrica: valor}}, bestModel).
    """
    # Separamos el pipeline: las etapas de features compartidas y, aparte, el estimador a ajustar.
    sample_pipeline = pipeline_factory(algorithm, param_list[0])
    stages = sample_pipeline.getStages()
    feature_stages, estimator = stages[:-1], stages[-1]
    features_col = estimator.getFeaturesCol()

    evaluator = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="rmse",
    )

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        mlflow.set_tags(tags)
        log_input_datasets(input_datasets)
        mlflow.log_params(extra_params)
        mlflow.log_param("cv_num_folds", num_folds)
        mlflow.log_param("cv_num_grid_points", len(param_list))

        progress(
            f"  [{run_name}] CV: {len(param_list)} grid × {num_folds} folds "
            f"= {len(param_list) * num_folds} fits..."
        )
        t0 = time.perf_counter()

        folds = train_df.randomSplit([1.0] * num_folds, seed=42)
        rmse_sums = [0.0] * len(param_list)

        for i in range(num_folds):
            fold_t = time.perf_counter()
            progress(
                f"  [{run_name}] fold {i + 1}/{num_folds}: entrenando etapas de features "
                f"+ cacheando features del fold..."
            )
            val_fold = folds[i]
            train_fold = reduce(
                DataFrame.unionByName,
                (folds[j] for j in range(num_folds) if j != i),
            )

            feat_model = Pipeline(stages=feature_stages).fit(train_fold)
            train_feat = feat_model.transform(train_fold) \
                .select(features_col, label_col).cache()
            val_feat = feat_model.transform(val_fold) \
                .select(features_col, label_col).cache()
            try:
                tr_n = train_feat.count()
                vl_n = val_feat.count()
                progress(
                    f"  [{run_name}] fold {i + 1}/{num_folds}: features listas "
                    f"(train {tr_n:,} / val {vl_n:,} filas); entrenando "
                    f"{len(param_list)} punto(s) de grid, {CV_PARALLELISM} a la vez..."
                )

                def _fit_eval(item):
                    idx, params = item
                    progress(
                        f"    [{run_name}] fold {i + 1} · grid "
                        f"{idx + 1}/{len(param_list)}: inicio entrenamiento {params}"
                    )
                    pt = time.perf_counter()
                    pm = {estimator.getParam(k): v for k, v in params.items()}
                    model = estimator.copy(pm).fit(train_feat)
                    rmse = evaluator.evaluate(model.transform(val_feat))
                    progress(
                        f"    [{run_name}] fold {i + 1} · grid "
                        f"{idx + 1}/{len(param_list)}: hecho val_rmse={rmse:.4f} "
                        f"({time.perf_counter() - pt:.1f}s)"
                    )
                    return idx, rmse

                # CV_PARALLELISM fits a la vez. pool.map bloquea, así que rmse_sums
                # solo se toca desde el hilo principal.
                with ThreadPoolExecutor(max_workers=CV_PARALLELISM) as pool:
                    for idx, rmse in pool.map(_fit_eval, enumerate(param_list)):
                        rmse_sums[idx] += rmse
            finally:
                train_feat.unpersist()
                val_feat.unpersist()
            progress(
                f"  [{run_name}] fold {i + 1}/{num_folds} hecho "
                f"({time.perf_counter() - fold_t:.1f}s)"
            )

        avg_metrics = [s / num_folds for s in rmse_sums]
        best_idx = min(range(len(avg_metrics)), key=lambda i: avg_metrics[i])
        best_params = param_list[best_idx]
        progress(
            f"  [{run_name}] CV hecho en {time.perf_counter() - t0:.1f}s — "
            f"ganador grid {best_idx + 1}/{len(param_list)} "
            f"avg_val_rmse={avg_metrics[best_idx]:.4f} {best_params}"
        )

        for i, avg in enumerate(avg_metrics):
            mlflow.log_metric(f"cv_avg_rmse_grid_{i}", float(avg))
        mlflow.log_metric("cv_best_avg_rmse", float(avg_metrics[best_idx]))
        mlflow.log_param("best_grid_idx", best_idx)
        for k, v in best_params.items():
            mlflow.log_param(f"best_{k}", v)

        # bestModel: reentrenamos el pipeline entero sobre todo train_df con los mejores parámetros.
        progress(f"  [{run_name}] reentrenando bestModel sobre todo train...")
        refit_t = time.perf_counter()
        best_model = pipeline_factory(algorithm, best_params).fit(train_df)
        progress(
            f"  [{run_name}] bestModel reentrenado en {time.perf_counter() - refit_t:.1f}s; "
            f"evaluando sobre {len(eval_splits)} split(s)..."
        )

        all_metrics: dict[str, dict[str, float]] = {}
        for split_name, split_df in eval_splits.items():
            metrics = evaluate_regression(best_model, split_df, label_col)
            log_split_metrics(metrics, split_name)
            all_metrics[split_name] = metrics
            progress(
                f"  [{run_name}] {split_name}: "
                f"rmse={metrics['rmse']:.4f} mae={metrics['mae']:.4f} "
                f"r2={metrics['r2']:.4f}"
            )

        progress(f"  [{run_name}] registrando mejor modelo...")
        mlflow.spark.log_model(
            best_model, artifact_path="model",
            pip_requirements=MODEL_PIP_REQUIREMENTS,
        )

    return run_id, best_params, all_metrics, best_model


# --- Baseline ---

def _fit_baseline_predictor(
    train_df: DataFrame, label_col: str, group_cols: list[str],
) -> tuple[DataFrame, float]:
    """Predictor por media de grupo. Devuelve (group_means_df, media global de respaldo)."""
    group_means = (
        train_df
        .groupBy(*group_cols)
        .agg(spark_mean(col(label_col)).alias("baseline_pred"))
    )
    global_mean = float(
        train_df.agg(
            spark_mean(col(label_col)).alias("global_mean")
        ).collect()[0]["global_mean"]
    )
    return group_means, global_mean


def _score_baseline(
    df: DataFrame, group_means: DataFrame, global_mean: float,
    label_col: str, group_cols: list[str],
) -> dict[str, float]:
    """Puntúa el predictor de media por grupo sobre `df` y devuelve {rmse, mae, r2}."""
    predictions = (
        df
        .join(broadcast(group_means), on=group_cols, how="left")
        .withColumn(
            "prediction",
            when(col("baseline_pred").isNull(), lit(global_mean))
            .otherwise(col("baseline_pred"))
            .cast(DoubleType()),
        )
        .select(label_col, "prediction")
        .cache()
    )
    try:
        return evaluate_predictions(predictions, label_col)
    finally:
        predictions.unpersist()


def compute_baseline_metrics(
    *,
    train_df: DataFrame,
    test_df: DataFrame,
    label_col: str,
    group_cols: list[str],
) -> dict[str, float]:
    """Baseline de media de la etiqueta por grupo, con la media global como respaldo para
    los grupos no vistos. Usamos la media y no la mediana porque es la constante óptima en RMSE por grupo."""
    group_means, global_mean = _fit_baseline_predictor(
        train_df, label_col, group_cols,
    )
    return _score_baseline(test_df, group_means, global_mean, label_col, group_cols)


def log_baseline_run(
    *,
    train_df: DataFrame,
    eval_splits: dict[str, DataFrame],
    label_col: str,
    group_cols: list[str],
    tags: dict,
    extra_params: dict,
    input_datasets: list[DatasetInput] | None = None,
) -> dict[str, float]:
    """Entrena con `train_df` y luego puntúa y registra cada split para que el run lleve
    el mismo conjunto de métricas que los runs de modelos. Devuelve las métricas de common_test."""
    progress(f"  [baseline] entrenando predictor media-por-grupo sobre {group_cols}...")
    group_means, global_mean = _fit_baseline_predictor(
        train_df, label_col, group_cols,
    )
    group_means = group_means.cache()  # puntuamos cada split contra esto
    try:
        per_split: dict[str, dict[str, float]] = {}
        with mlflow.start_run(run_name="baseline"):
            mlflow.set_tags(tags)
            log_input_datasets(input_datasets)
            mlflow.log_params({
                **extra_params,
                "baseline_group_cols": ",".join(group_cols),
            })
            for split_name, split_df in eval_splits.items():
                metrics = _score_baseline(
                    split_df, group_means, global_mean, label_col, group_cols,
                )
                log_split_metrics(metrics, split_name)
                per_split[split_name] = metrics
                progress(
                    f"  [baseline] {split_name}: rmse={metrics['rmse']:.4f} "
                    f"mae={metrics['mae']:.4f} r2={metrics['r2']:.4f}"
                )
    finally:
        group_means.unpersist()
    return per_split.get("common_test", next(iter(per_split.values())))


# --- Latencia de inferencia ---

def measure_inference_latency(
    model,
    df: DataFrame,
    n_rows: int = INFERENCE_LATENCY_SAMPLE_ROWS,
) -> tuple[float, float, int]:
    """Cronometra model.transform sobre una muestra cacheada de `n_rows`, tras una pasada
    de calentamiento. Devuelve (segundos, ms_por_fila, n_filas_reales). Es latencia del
    clúster de Spark: una comprobación de regresión entre champions, no un SLO de producción."""
    sample = df.limit(n_rows).cache()
    try:
        actual_n = sample.count()
        if actual_n == 0:
            return 0.0, float("nan"), 0
        # Calentamiento: el primer transform absorbe el coste de JIT/broadcast.
        model.transform(sample).select("prediction").count()
        t0 = time.perf_counter()
        model.transform(sample).select("prediction").collect()
        elapsed = time.perf_counter() - t0
        return elapsed, (elapsed * 1000.0) / actual_n, actual_n
    finally:
        sample.unpersist()


# --- Registro de modelos ---

def prune_old_versions(
    client: MlflowClient,
    name: str,
    keep_recent: int = REGISTRY_MAX_VERSIONS,
    protected_aliases: tuple[str, ...] = REGISTRY_PROTECTED_ALIASES,
) -> list[str]:
    """Borra las versiones más allá de las `keep_recent` más recientes. Nunca toca una versión
    que tenga un alias protegido: el alias tiene que seguir siendo cargable."""
    versions = client.search_model_versions(f"name='{name}'")
    if not versions:
        return []
    versions = sorted(versions, key=lambda v: int(v.version), reverse=True)

    protected: set[str] = set()
    for alias in protected_aliases:
        try:
            mv = client.get_model_version_by_alias(name, alias)
            protected.add(mv.version)
        except MlflowException:
            pass

    top_recent = {v.version for v in versions[:keep_recent]}
    keep = top_recent | protected
    deleted: list[str] = []
    for v in versions:
        if v.version in keep:
            continue
        try:
            client.delete_model_version(name=name, version=v.version)
            deleted.append(v.version)
            print(
                f"  Podada v{v.version} de {name} "
                f"(se conservan {len(keep)} versiones: top {keep_recent} más recientes "
                f"+ con alias)"
            )
        except MlflowException as e:
            print(f"  No se pudo podar v{v.version}: {e}")
    return deleted


def _run_metric(client: MlflowClient, run_id: str, metric: str) -> float | None:
    """Métrica de un run, o None si el run ya no existe o no la registró."""
    try:
        return client.get_run(run_id).data.metrics[metric]
    except (MlflowException, KeyError):
        return None


def register_candidates(
    *,
    name: str,
    candidates: list[dict],
    champion_run_id: str,
    promotion_metric: str,
    build_version_description: Callable[[dict], str],
    build_version_tags: Callable[[dict], dict[str, str]],
    registered_model_description: str,
    registered_model_tags: dict[str, str],
    latency_model=None,
    latency_sample_df: DataFrame | None = None,
    max_versions: int = REGISTRY_MAX_VERSIONS,
    protected_aliases: tuple[str, ...] = REGISTRY_PROTECTED_ALIASES,
) -> dict[str, tuple]:
    """Registra cada candidato como una versión nueva de `name`.

    Cada candidato es un dict con al menos "run_id". El candidato de champion_run_id se
    queda con @champion solo si su `promotion_metric` es estrictamente menor que la del
    champion actual; en caso contrario es un challenger, y todo lo demás también lo es.
    @challenger apunta a la mejor versión que no sea champion. Cuando hay una promoción, el
    nuevo @champion recibe los tags de latencia (si se pasaron latency_model/df) y después
    se poda el registro.
    Devuelve {run_id: (ModelVersion, status)}.
    """
    client = MlflowClient()

    # Creamos el modelo registrado la primera vez que lo vemos.
    try:
        client.get_registered_model(name)
    except MlflowException:
        client.create_registered_model(
            name=name,
            description=registered_model_description or "",
            tags=registered_model_tags or {},
        )

    deployment_date = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # register_model bloquea hasta que la versión está READY.
    registered: dict[str, "mlflow.entities.model_registry.ModelVersion"] = {}
    for n, c in enumerate(candidates, start=1):
        progress(
            f"  Registrando candidato {n}/{len(candidates)} "
            f"({c.get('run_name', c['run_id'])}) como nueva versión de {name} "
            f"(esperando READY)..."
        )
        mv = mlflow.register_model(
            model_uri=f"runs:/{c['run_id']}/model",
            name=name,
        )
        client.update_model_version(
            name=name, version=mv.version,
            description=build_version_description(c),
        )
        version_tags_full = {
            **build_version_tags(c),
            "deployment_date": deployment_date,
            "validation_status": "challenger",  # por defecto, puede cambiar más abajo
        }
        for k, v in version_tags_full.items():
            client.set_model_version_tag(
                name=name, version=mv.version, key=k, value=str(v),
            )
        registered[c["run_id"]] = mv
        progress(f"  Candidato {n}/{len(candidates)} registrado como v{mv.version}.")

    def _set_status(version: str, status: str) -> None:
        client.set_model_version_tag(
            name=name, version=version, key="validation_status", value=status,
        )

    contender_mv = registered[champion_run_id]
    contender_metric = _run_metric(client, champion_run_id, promotion_metric)

    cur_champ_version: str | None = None
    cur_champ_metric: float | None = None
    try:
        cur_champ = client.get_model_version_by_alias(name, "champion")
    except MlflowException:
        cur_champ = None  # todavía no hay champion

    if cur_champ is None:
        promoted = True  # el aspirante gana por defecto
    else:
        cur_champ_version = cur_champ.version
        # El run del champion puede haberse borrado o no llevar la métrica (versiones
        # registradas con otro esquema de métricas): sin referencia con la que comparar,
        # promovemos y lo avisamos en vez de reventar con el registro a medias.
        cur_champ_metric = _run_metric(client, cur_champ.run_id, promotion_metric)
        if cur_champ_metric is None:
            print(
                f"  El champion actual (v{cur_champ_version}) no expone "
                f"'{promotion_metric}' en su run: no hay comparación posible, "
                f"se promueve al aspirante."
            )
            promoted = True
        else:
            promoted = contender_metric < cur_champ_metric

    if promoted:
        client.set_registered_model_alias(name, "champion", contender_mv.version)
        _set_status(contender_mv.version, "champion")
        if cur_champ_version is not None:
            _set_status(cur_champ_version, "challenger")
            print(
                f"  v{contender_mv.version} promovida a @champion "
                f"({promotion_metric} {contender_metric:.4f} < "
                f"el de v{cur_champ_version} {cur_champ_metric:.4f}); "
                f"v{cur_champ_version} etiquetada validation_status=challenger"
            )
        else:
            print(
                f"  v{contender_mv.version} establecida como @champion "
                f"(primera versión de {name})"
            )
    else:
        # Ya quedó etiquetado como challenger arriba, así que dejamos el tag como está.
        print(
            f"  v{contender_mv.version} se queda como challenger "
            f"({promotion_metric} {contender_metric:.4f} >= "
            f"el de v{cur_champ_version} {cur_champ_metric:.4f})"
        )

    # @challenger apunta al mejor candidato de esta ejecución que NO se haya quedado con
    # @champion. Si el aspirante no fue promovido sigue siendo candidato — y de hecho es el
    # mejor por promotion_metric —, así que entra en el sorteo.
    challenger_pool = [
        c["run_id"] for c in candidates
        if not (promoted and c["run_id"] == champion_run_id)
    ]
    challenger_alias_version: str | None = None
    if challenger_pool:
        run_metrics = {
            rid: _run_metric(client, rid, promotion_metric) or float("inf")
            for rid in challenger_pool
        }
        best_other = min(run_metrics, key=run_metrics.get)
        challenger_alias_version = registered[best_other].version

    if challenger_alias_version is not None:
        client.set_registered_model_alias(
            name, "challenger", challenger_alias_version,
        )
        print(f"  @challenger -> v{challenger_alias_version}")

    # Tags de latencia solo en el @champion recién promovido: es el que se va a servir.
    if promoted and latency_model is not None and latency_sample_df is not None:
        elapsed_s, ms_per_row, n = measure_inference_latency(
            latency_model, latency_sample_df,
        )
        client.set_model_version_tag(
            name=name, version=contender_mv.version,
            key="inference_latency_ms_per_row", value=f"{ms_per_row:.3f}",
        )
        client.set_model_version_tag(
            name=name, version=contender_mv.version,
            key="inference_latency_sample_n", value=str(n),
        )
        client.set_model_version_tag(
            name=name, version=contender_mv.version,
            key="inference_latency_total_s", value=f"{elapsed_s:.3f}",
        )
        print(
            f"  v{contender_mv.version} latencia de inferencia: "
            f"{ms_per_row:.3f} ms/fila en n={n} (total {elapsed_s:.2f}s)"
        )

    # Mantenemos el registro pequeño; las versiones con alias sobreviven a la poda.
    prune_old_versions(
        client, name, keep_recent=max_versions,
        protected_aliases=protected_aliases,
    )

    result: dict[str, tuple] = {}
    for c in candidates:
        mv = registered[c["run_id"]]
        if c["run_id"] == champion_run_id and promoted:
            status = "champion"
        else:
            status = "challenger"
        result[c["run_id"]] = (mv, status)
    return result
