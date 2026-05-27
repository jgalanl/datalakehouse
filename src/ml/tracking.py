"""MLflow tracking: Spark/MLflow setup, run helpers, baseline, registry.

Bundles everything that touches MLflow into one module: session setup
(`get_ml_spark`, `setup_mlflow`), evaluation helpers, the run-logging
wrappers (`train_and_log_run`, `tune_with_cv`), the mean-per-group baseline,
and the multi-candidate registry helper (`register_candidates`).
"""

import os
import time
from datetime import datetime, timezone
from typing import Callable

# MLflow + MinIO env. spark-master inherits AWS_* from docker-compose but not
# MLFLOW_*; setting them before importing mlflow lets boto3 reach MinIO when
# artifacts are uploaded.
os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")

import mlflow  # noqa: E402
import mlflow.spark  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from mlflow.exceptions import MlflowException  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.tuning import CrossValidator  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.functions import broadcast, col, lit, mean as spark_mean, when  # noqa: E402
from pyspark.sql.types import DoubleType  # noqa: E402

from common.spark_factory import get_spark  # noqa: E402
from common import nessie_manager as nessie  # noqa: E402

MLFLOW_TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]

# Explicit env spec for logged models. MLflow's automatic pip-requirements
# inference fails inside the spark-master container (no usable conda env to
# introspect) and falls back to a bare ['pyspark==3.5.8']. Pinning this list
# silences that warning and makes the saved env reproducible.
MODEL_PIP_REQUIREMENTS = ["pyspark==3.5.8", "mlflow==2.20.3"]


def progress(msg: str) -> None:
    """Timestamped, flushed progress line. spark-submit buffers stdout, so
    long Spark stages look frozen without an explicit flush."""
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def get_ml_spark(app_name: str) -> SparkSession:
    """Build a Spark session pinned to main (gold ref)."""
    spark = get_spark(app_name=app_name)
    nessie.set_spark_branch(spark, "main")
    return spark


def setup_mlflow(experiment_name: str) -> None:
    """Point MLflow at the tracking server and activate `experiment_name`.

    If the experiment was soft-deleted in the UI, mlflow.set_experiment would
    raise — MLflow won't reuse a deleted name. Restore it so the script
    survives a half-done cleanup; delete individual runs in the UI if needed.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    exp = client.get_experiment_by_name(experiment_name)
    if exp is not None and exp.lifecycle_stage == "deleted":
        client.restore_experiment(exp.experiment_id)
        progress(f"Restored soft-deleted MLflow experiment '{experiment_name}'.")
    mlflow.set_experiment(experiment_name)


# ----- Evaluation -----------------------------------------------------------

REGRESSION_METRICS: tuple[str, ...] = ("rmse", "mae", "r2")

# How many rows of common_test to score when timing the @champion's inference
# latency. 1000 is enough for a stable ms/row figure across runs without
# inflating the Phase 3 registration cost.
INFERENCE_LATENCY_SAMPLE_ROWS: int = 1000

# Hard cap on retained model versions per registered model. Anything beyond
# this — except versions carrying an `@champion` / `@challenger` alias — is
# pruned at the end of `register_candidates`. Each train.py run registers
# ~5 versions, so 20 is roughly the last four invocations' worth.
REGISTRY_MAX_VERSIONS: int = 20
REGISTRY_PROTECTED_ALIASES: tuple[str, ...] = ("champion", "challenger")


def evaluate_predictions(
    predictions_df: DataFrame, label_col: str,
) -> dict[str, float]:
    """Return {rmse, mae, r2} for a DataFrame that already has `label_col` and
    a `prediction` column. Pure helper — caller manages caching (the three
    evaluators each do a full pass, so caching upstream is worth it)."""
    return {
        m: float(RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName=m,
        ).evaluate(predictions_df))
        for m in REGRESSION_METRICS
    }


def evaluate_regression(
    model, df: DataFrame, label_col: str,
) -> dict[str, float]:
    """Score `model` on `df` — returns {rmse, mae, r2}."""
    predictions = model.transform(df).select(label_col, "prediction").cache()
    try:
        return evaluate_predictions(predictions, label_col)
    finally:
        predictions.unpersist()


def log_split_metrics(metrics: dict[str, float], split: str) -> None:
    for name, value in metrics.items():
        mlflow.log_metric(f"{split}_{name}", value)


# ----- Training helpers -----------------------------------------------------

def train_and_log_run(
    *,
    splits: dict[str, DataFrame],
    label_col: str,
    pipeline: Pipeline,
    run_name: str,
    tags: dict,
    extra_params: dict | None = None,
) -> tuple[str, dict[str, dict[str, float]], object]:
    """Fit pipeline on splits['train'], evaluate on every split, log to MLflow.

    Returns (run_id, per-split metrics dict, fitted PipelineModel) — the
    in-memory model is returned so callers can reuse it (e.g. for inference
    latency measurement) without paying the cost of reloading from MLflow.
    """
    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        mlflow.set_tags(tags)
        if extra_params:
            mlflow.log_params(extra_params)

        progress(f"  [{run_name}] fitting ({len(pipeline.getStages())} stages)...")
        t0 = time.perf_counter()
        model = pipeline.fit(splits["train"])
        progress(f"  [{run_name}] fit done in {time.perf_counter() - t0:.1f}s")

        all_metrics: dict[str, dict[str, float]] = {}
        for split_name, split_df in splits.items():
            t1 = time.perf_counter()
            metrics = evaluate_regression(model, split_df, label_col)
            log_split_metrics(metrics, split_name)
            all_metrics[split_name] = metrics
            progress(f"  [{run_name}] {split_name} in {time.perf_counter() - t1:.1f}s")

        progress(f"  [{run_name}] logging model...")
        t2 = time.perf_counter()
        mlflow.spark.log_model(
            model, artifact_path="model", pip_requirements=MODEL_PIP_REQUIREMENTS,
        )
        progress(f"  [{run_name}] model logged in {time.perf_counter() - t2:.1f}s")
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
) -> tuple[str, dict, dict[str, dict[str, float]], object]:
    """CrossValidator search over `param_list` for one algorithm.

    The CV is built once with the first param dict as a base pipeline, then
    each dict becomes an explicit ParamMap targeting the estimator stage.
    bestModel is the pipeline refit on the full train_df with the winning
    params — we evaluate it on every entry in `eval_splits` (val,
    common_test, ...) and log it to MLflow.

    Returns (run_id, best_params, {split: {metric: value}}, bestModel) — the
    in-memory PipelineModel is returned alongside its run/metrics so callers
    don't have to reload it from MLflow when measuring inference latency.
    """
    sample_pipeline = pipeline_factory(algorithm, param_list[0])
    estimator = sample_pipeline.getStages()[-1]

    param_maps = []
    for params in param_list:
        pm = {estimator.getParam(k): v for k, v in params.items()}
        param_maps.append(pm)

    evaluator = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="rmse",
    )
    cv = CrossValidator(
        estimator=sample_pipeline,
        estimatorParamMaps=param_maps,
        evaluator=evaluator,
        numFolds=num_folds,
        seed=42,
        # The cluster has 2 worker JVMs (spark-worker-1, spark-worker-2), so the
        # driver can submit 2 fits concurrently and each lands on its own executor.
        # Going higher than 2 just queues fits on the same executors and contends
        # for the cached splits' memory.
        parallelism=2,
        collectSubModels=False,
    )

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        mlflow.set_tags(tags)
        mlflow.log_params(extra_params)
        mlflow.log_param("cv_num_folds", num_folds)
        mlflow.log_param("cv_num_grid_points", len(param_list))

        progress(
            f"  [{run_name}] CV: {len(param_list)} grid × {num_folds} folds "
            f"= {len(param_list) * num_folds} fits..."
        )
        t0 = time.perf_counter()
        cv_model = cv.fit(train_df)
        progress(f"  [{run_name}] CV done in {time.perf_counter() - t0:.1f}s")

        avg_metrics = cv_model.avgMetrics
        best_idx = min(range(len(avg_metrics)), key=lambda i: avg_metrics[i])
        best_params = param_list[best_idx]
        for i, avg in enumerate(avg_metrics):
            mlflow.log_metric(f"cv_avg_rmse_grid_{i}", float(avg))
        mlflow.log_metric("cv_best_avg_rmse", float(avg_metrics[best_idx]))
        mlflow.log_param("best_grid_idx", best_idx)
        for k, v in best_params.items():
            mlflow.log_param(f"best_{k}", v)

        all_metrics: dict[str, dict[str, float]] = {}
        for split_name, split_df in eval_splits.items():
            metrics = evaluate_regression(cv_model.bestModel, split_df, label_col)
            log_split_metrics(metrics, split_name)
            all_metrics[split_name] = metrics

        progress(f"  [{run_name}] logging best model...")
        mlflow.spark.log_model(
            cv_model.bestModel, artifact_path="model",
            pip_requirements=MODEL_PIP_REQUIREMENTS,
        )

    return run_id, best_params, all_metrics, cv_model.bestModel


# ----- Baseline -------------------------------------------------------------

def compute_baseline_metrics(
    *,
    train_df: DataFrame,
    test_df: DataFrame,
    label_col: str,
    group_cols: list[str],
) -> dict[str, float]:
    """Mean-of-label-per-group baseline — predicts `mean(label)` for each
    (group_cols) bucket seen in train, falling back to the global mean for
    unseen buckets in test.

    Mean (not median) because RMSE is our reported metric: among all
    constant-per-group predictors, the per-group mean is the
    RMSE-optimal one. Using the median (MAE-optimal) would inflate the
    model's lift over baseline by handicapping the baseline on the metric
    we actually report.

    A floor reference: any trained model that doesn't beat this on RMSE
    isn't learning anything beyond per-group averages.
    """
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
    predictions = (
        test_df
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


def log_baseline_run(
    *,
    train_df: DataFrame,
    test_df: DataFrame,
    label_col: str,
    group_cols: list[str],
    tags: dict,
    extra_params: dict,
) -> dict[str, float]:
    """Compute the mean-per-group baseline on `test_df` and log it as a
    `baseline` MLflow run. Returns {rmse, mae, r2}."""
    metrics = compute_baseline_metrics(
        train_df=train_df, test_df=test_df,
        label_col=label_col, group_cols=group_cols,
    )
    with mlflow.start_run(run_name="baseline"):
        mlflow.set_tags(tags)
        mlflow.log_params({
            **extra_params,
            "baseline_group_cols": ",".join(group_cols),
        })
        for metric_name, value in metrics.items():
            mlflow.log_metric(f"common_test_{metric_name}", value)
    return metrics


# ----- Inference latency ----------------------------------------------------

def measure_inference_latency(
    model,
    df: DataFrame,
    n_rows: int = INFERENCE_LATENCY_SAMPLE_ROWS,
) -> tuple[float, float, int]:
    """Time how long `model.transform(...)` takes on a small sample of `df`.

    Returns (elapsed_seconds, ms_per_row, actual_n_rows). The sample is
    cached and a warm-up pass is run before timing, so the measurement
    isolates the prediction cost from broadcast/JIT/scheduler overhead.

    Numbers are Spark-cluster latency (full distributed roundtrip) — useful
    as a regression check against the previous champion ("did v40 suddenly
    get 5× slower than v39?"), not as a production-serving SLO.
    """
    sample = df.limit(n_rows).cache()
    try:
        actual_n = sample.count()
        if actual_n == 0:
            return 0.0, float("nan"), 0
        # Warm-up — first transform pays the JIT/broadcast cost.
        model.transform(sample).select("prediction").count()
        t0 = time.perf_counter()
        model.transform(sample).select("prediction").collect()
        elapsed = time.perf_counter() - t0
        return elapsed, (elapsed * 1000.0) / actual_n, actual_n
    finally:
        sample.unpersist()


# ----- Model registry -------------------------------------------------------

def prune_old_versions(
    client: MlflowClient,
    name: str,
    keep_recent: int = REGISTRY_MAX_VERSIONS,
    protected_aliases: tuple[str, ...] = REGISTRY_PROTECTED_ALIASES,
) -> list[str]:
    """Delete versions of `name` beyond the `keep_recent` most recent.

    Any version carrying one of `protected_aliases` is always kept, even if
    it falls outside the top-N most-recent. That preserves the cross-run
    audit trail (the alias resolves to a real, loadable version) while
    capping the registry growth — without this every train.py run leaks
    ~5 versions forever.
    """
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
                f"  Pruned v{v.version} from {name} "
                f"(retain {len(keep)} versions: top {keep_recent} most recent "
                f"+ aliased)"
            )
        except MlflowException as e:
            print(f"  Failed to prune v{v.version}: {e}")
    return deleted


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
    """Register every entry in `candidates` as a new version of `name`.

    Each candidate is a dict with at least a "run_id" key (the rest is
    passed verbatim to `build_version_description` / `build_version_tags`).

    On first creation of `name`, seed the registered model with the
    `registered_model_*` metadata. Later invocations do NOT overwrite that —
    manual UI edits survive.

    The candidate whose run_id == `champion_run_id` is the contender for
    @champion (the within-run winner). Promotion rule (lower
    `promotion_metric` is better): contender's `promotion_metric` strictly
    below the previous @champion's value → contender gets @champion +
    `validation_status="champion"`, previous champion stays registered and
    is tagged `validation_status="challenger"`. Otherwise the contender is
    tagged `validation_status="challenger"`.

    Every other candidate is always tagged `validation_status="challenger"`.

    The @challenger alias points to the best non-champion version registered
    in this round (lowest `promotion_metric`).

    On promotion, if `latency_model` + `latency_sample_df` are provided, the
    new @champion version gets three latency tags
    (`inference_latency_ms_per_row`, `inference_latency_sample_n`,
    `inference_latency_total_s`). The measurement uses
    `measure_inference_latency` (1000-row sample by default) — see that
    helper for what the numbers actually represent.

    Finally calls `prune_old_versions(name, keep_recent=max_versions)` to
    cap registry growth at `max_versions` non-aliased versions. The
    `@champion` / `@challenger` aliased versions are always kept.

    Returns: {run_id: (ModelVersion, status)} for each candidate.
    """
    client = MlflowClient()

    # First-creation hook.
    try:
        client.get_registered_model(name)
    except MlflowException:
        client.create_registered_model(
            name=name,
            description=registered_model_description or "",
            tags=registered_model_tags or {},
        )

    deployment_date = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Register each candidate. mlflow.register_model waits until READY
    # (default await_registration_for=300s).
    registered: dict[str, "mlflow.entities.model_registry.ModelVersion"] = {}
    for c in candidates:
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
            "validation_status": "challenger",  # default; overridden below
        }
        for k, v in version_tags_full.items():
            client.set_model_version_tag(
                name=name, version=mv.version, key=k, value=str(v),
            )
        registered[c["run_id"]] = mv

    def _set_status(version: str, status: str) -> None:
        client.set_model_version_tag(
            name=name, version=version, key="validation_status", value=status,
        )

    contender_mv = registered[champion_run_id]
    contender_metric = client.get_run(champion_run_id).data.metrics[promotion_metric]

    cur_champ_version: str | None = None
    cur_champ_metric: float | None = None
    try:
        cur_champ = client.get_model_version_by_alias(name, "champion")
        cur_champ_version = cur_champ.version
        cur_champ_metric = client.get_run(cur_champ.run_id).data.metrics[promotion_metric]
        promoted = contender_metric < cur_champ_metric
    except MlflowException:
        # First-ever registration: contender becomes champion unconditionally.
        promoted = True

    if promoted:
        client.set_registered_model_alias(name, "champion", contender_mv.version)
        _set_status(contender_mv.version, "champion")
        if cur_champ_version is not None:
            _set_status(cur_champ_version, "challenger")
            print(
                f"  v{contender_mv.version} promoted to @champion "
                f"({promotion_metric} {contender_metric:.4f} < "
                f"v{cur_champ_version}'s {cur_champ_metric:.4f}); "
                f"v{cur_champ_version} tagged validation_status=challenger"
            )
        else:
            print(
                f"  v{contender_mv.version} set as @champion "
                f"(first version of {name})"
            )
    else:
        # Already defaulted to challenger; nothing to change on the tag side.
        print(
            f"  v{contender_mv.version} stays as challenger "
            f"({promotion_metric} {contender_metric:.4f} >= "
            f"v{cur_champ_version}'s {cur_champ_metric:.4f})"
        )

    # @challenger alias → best non-champion candidate from this run. If the
    # contender stayed as challenger, that's the contender itself (it's by
    # definition the best of the new batch on promotion_metric).
    non_champion_run_ids = [c["run_id"] for c in candidates if c["run_id"] != champion_run_id]
    challenger_alias_version: str | None = None
    if non_champion_run_ids:
        run_metrics = {
            rid: client.get_run(rid).data.metrics[promotion_metric]
            for rid in non_champion_run_ids
        }
        best_other = min(run_metrics, key=run_metrics.get)
        challenger_alias_version = registered[best_other].version
    elif not promoted:
        challenger_alias_version = contender_mv.version

    if challenger_alias_version is not None:
        client.set_registered_model_alias(
            name, "challenger", challenger_alias_version,
        )
        print(f"  @challenger -> v{challenger_alias_version}")

    # Inference latency tag on the new @champion. Only measured on promotion
    # (not on every candidate) — it's a regression check against the previous
    # champion ("did the cluster cost-per-prediction jump?"), and that signal
    # only makes sense for the version that will actually serve.
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
            f"  v{contender_mv.version} inference latency: "
            f"{ms_per_row:.3f} ms/row on n={n} (total {elapsed_s:.2f}s)"
        )

    # Registry cleanup — cap retained versions at `max_versions`, never
    # touching anything carrying an `@champion`/`@challenger` alias.
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
