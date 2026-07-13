"""Script de entrenamiento unificado: spark-submit src/ml/train.py <fare|duration|demand> [--fast].
Cada ejecución registra todo en un único experimento de MLflow:
  Fase 0    descarta las categóricas redundantes (p. ej. borough, derivable de location_id).
  baseline  media(etiqueta) por grupo — el suelo óptimo en RMSE de una constante por grupo.
  Fase 1    un CV por algoritmo sobre su grid completo; gana el menor val_rmse.
  Fase 2    más datos A (subconjunto) vs B (el ganador, reutilizado); ambos puntúan sobre TEST_YEAR.
  Fase 3    registra los 4 candidatos; el ganador se queda con @champion solo si su
            val_rmse supera al del champion del registro; si no, sigue de challenger.

A tener en cuenta: val_rmse guía cada decisión (common_test queda reservado y solo se
reporta), el split es un holdout del último año (common_test = TEST_YEAR), cada versión se
fija con nessie_main_hash + source_snapshot_id, el @champion recibe un tag de latencia de
inferencia, y el registro se poda a REGISTRY_MAX_VERSIONS conservando las versiones con alias.
"""

import sys

import mlflow
from mlflow import MlflowClient
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from common import nessie_manager as nessie
from ml.algorithms import (
    ALGORITHMS,
    CV_NUM_FOLDS,
    FAST_CV_NUM_FOLDS,
    FAST_FULL_PARAM_GRIDS,
    FAST_SAMPLE_FRACTION,
    FULL_PARAM_GRIDS,
    GLR_FAMILY_PRESETS,
    SAMPLE_FRACTION,
    algorithm_pipeline_config,
    fast_mode_enabled,
    make_estimator,
)
from ml.pipelines import build_regression_pipeline
from ml.splits import (
    SUBSET_A_YEARS,
    TRAIN_VAL_YEARS,
    add_cyclical_features,
    make_splits,
    resolve_numerical_cols,
)
from ml.tracking import (
    MLFLOW_TRACKING_URI,
    REGISTRY_MAX_VERSIONS,
    get_ml_spark,
    log_baseline_run,
    progress,
    register_candidates,
    setup_mlflow,
    train_and_log_run,
    tune_with_cv,
)


CREATED_BY = "jgalanllano"

# Conjunto de features de yellow_taxi_trips, compartido por los casos fare y duration.
# El año (pickup_year) NO es feature: es la clave del holdout, así que en common_test toma un
# valor (TEST_YEAR) que no aparece en ninguna fila de entrenamiento. Los árboles no extrapolan
# —2025 caería en la rama terminal de 2024— y GLR sí, aplicando un factor exp(beta*delta_año)
# a todas las predicciones del año de test: un sesgo sistemático a cambio de nada. La
# estacionalidad ya la cubren mes/semana/día/hora.
TRIP_NUMERICAL_COLS = [
    "trip_distance", "passenger_count",
    "pickup_month", "pickup_week",
    "pickup_dow", "pickup_hour",
]
TRIP_CATEGORICAL_COLS = [
    "pu_location_id", "do_location_id",
    "pu_borough", "do_borough",
    "vendor_id", "ratecode_id", "payment_type",
]
TRIP_BASELINE_GROUP_COLS = ["pu_location_id", "pickup_hour"]

TRIP_REDUNDANT_CATEGORICALS = ["pu_borough", "do_borough"]

USE_CASES: dict[str, dict] = {
    "fare": {
        "experiment": "fare_prediction",
        "app_name": "ml-fare-prediction",
        "source": "nessie.nyc.yellow_taxi_trips",
        "label_col": "fare_amount",
        "year_col": "pickup_year",
        "numerical_cols": TRIP_NUMERICAL_COLS,
        "categorical_cols": TRIP_CATEGORICAL_COLS,
        "glr_family": "gamma",
        "project_name": "NYC Taxi Fare Prediction",
        "baseline_group_cols": TRIP_BASELINE_GROUP_COLS,
        "label_filter_predicate": None,
        "redundant_categoricals": TRIP_REDUNDANT_CATEGORICALS,
    },
    "duration": {
        "experiment": "trip_duration",
        "app_name": "ml-trip-duration",
        "source": "nessie.nyc.yellow_taxi_trips",
        "label_col": "trip_duration_minutes",
        "year_col": "pickup_year",
        "numerical_cols": TRIP_NUMERICAL_COLS,
        "categorical_cols": TRIP_CATEGORICAL_COLS,
        "glr_family": "gamma",
        "project_name": "NYC Taxi Trip Duration Prediction",
        "baseline_group_cols": TRIP_BASELINE_GROUP_COLS,
        "label_filter_predicate": lambda c: c > 0,
        "redundant_categoricals": TRIP_REDUNDANT_CATEGORICALS,
    },
    "demand": {
        "experiment": "demand_forecast",
        "app_name": "ml-demand-forecast",
        "source": "nessie.nyc.pickup_demand_zone_hour",
        "label_col": "n_pickups",
        "year_col": "hour_year",
        # hour_year fuera por la misma razón que pickup_year (ver TRIP_NUMERICAL_COLS).
        "numerical_cols": [
            "hour_month", "hour_week",
            "hour_dow", "hour_of_day",
        ],
        "categorical_cols": [
            "pu_location_id", "pu_borough", "pu_service_zone",
        ],
        "glr_family": "poisson",      # recuentos enteros no negativos
        "project_name": "NYC Taxi Pickup Demand Forecast",
        "baseline_group_cols": ["pu_location_id", "hour_of_day"],
        "label_filter_predicate": None,
        "redundant_categoricals": ["pu_borough", "pu_service_zone"],
    },
}


def _parse_cli(argv: list[str]) -> tuple[str, bool]:
    """Devuelve (use_case, fast). Sale con un mensaje de uso si la entrada es incorrecta."""
    positional = [a for a in argv if not a.startswith("--")]
    if len(positional) != 1 or positional[0] not in USE_CASES:
        names = "|".join(USE_CASES)
        print(f"Uso: spark-submit src/ml/train.py <{names}> [--fast]")
        sys.exit(2)
    return positional[0], fast_mode_enabled(argv)


def _build_pipeline(algorithm: str, params: dict, cfg: dict):
    """Construye el pipeline de features + el estimador para `algorithm` según cfg."""
    pipeline_config = algorithm_pipeline_config(algorithm)
    numerical_cols = resolve_numerical_cols(algorithm, cfg["numerical_cols"])

    estimator_params = dict(params)
    if algorithm == "glr" and "family" not in estimator_params:
        estimator_params.update(GLR_FAMILY_PRESETS[cfg["glr_family"]])

    return build_regression_pipeline(
        numerical_cols=numerical_cols,
        categorical_cols=cfg["categorical_cols"],
        label_col=cfg["label_col"],
        estimator=make_estimator(algorithm, **estimator_params),
        **pipeline_config,
    )


def _print_metrics(metrics: dict[str, dict[str, float]]) -> None:
    for split, m in metrics.items():
        parts = "  ".join(f"{n}={v:.4f}" for n, v in m.items())
        print(f"  {split:<12} {parts}")


def _make_dataset(df: DataFrame, name: str, cfg: dict):
    """Envuelve un split como Dataset de MLflow. El digest es el hash semántico del plan
    lógico (sin escaneo). Sin `version=` — eso es solo para Delta; el snapshot lo fijamos
    a través de source_snapshot_id."""
    return mlflow.data.from_spark(
        df,
        table_name=cfg["source"],
        name=name,
        targets=cfg["label_col"],
    )


def _common_tags(cfg: dict, phase: str, extra: dict | None = None) -> dict:
    tags = {
        "source_table": cfg["source"],
        "label": cfg["label_col"],
        "year_col": cfg["year_col"],
        "phase": phase,
        "project_name": cfg["project_name"],
        "created_by": CREATED_BY,
        # Se pone en cada run para que la columna nunca esté dispersa; solo la Fase 2 fija A/B.
        "more_data_subset": "none",
        # Los años del pool de entrenamiento (more-data-A lo sobrescribe con su subconjunto).
        "training_data": str(TRAIN_VAL_YEARS),
    }
    if extra:
        tags.update(extra)
    return tags


def main() -> None:
    use_case, fast = _parse_cli(sys.argv[1:])
    cfg = USE_CASES[use_case]
    sample_fraction = FAST_SAMPLE_FRACTION if fast else SAMPLE_FRACTION
    full_grids = FAST_FULL_PARAM_GRIDS if fast else FULL_PARAM_GRIDS
    cv_folds = FAST_CV_NUM_FOLDS if fast else CV_NUM_FOLDS

    progress(f"Iniciando Spark para use_case={use_case!r} fast={fast}...")
    spark = get_ml_spark(app_name=cfg["app_name"])
    try:
        progress("Spark listo. Configurando MLflow...")
        setup_mlflow(cfg["experiment"])
        if fast:
            progress(
                f"MODO RÁPIDO — muestreando {FAST_SAMPLE_FRACTION:.0%} de gold, "
                "grids/folds mínimos. Solo prueba de humo; métricas sin valor."
            )
        else:
            progress(
                f"Muestreando {SAMPLE_FRACTION:.0%} de gold para entrenar "
                "(acota el coste del CV; cada fold sigue con millones de filas)."
            )

        # ---- Fase 0: descartamos las categóricas redundantes (derivables de location_id) ----
        redundant = list(cfg["redundant_categoricals"])
        cfg = {**cfg, "categorical_cols": [
            c for c in cfg["categorical_cols"] if c not in redundant
        ]}
        print("\n=== Fase 0: descartando categóricas redundantes ===")
        print(f"  descartadas: {redundant}")
        print(f"  categóricas restantes: {cfg['categorical_cols']}")

        # ---- Carga + proyección + muestreo opcional + features cíclicas ----
        # Fijamos el estado de los datos (hash de main en Nessie + snapshot id del origen)
        # en cada versión, para que más adelante se pueda reconstruir la entrada exacta.
        nessie_main_hash = nessie.current_ref_hash(spark, "main") or ""
        source_snapshot_id = nessie.latest_snapshot_id(spark, cfg["source"])
        progress(
            f"Estado de datos fijado: nessie main={nessie_main_hash[:12] or 'unknown'}, "
            f"{cfg['source']} snapshot_id={source_snapshot_id}"
        )

        progress(f"Leyendo {cfg['source']}...")
        # year_col se proyecta aunque no sea feature: make_splits y la Fase 2 filtran por él.
        cols_to_keep = [
            cfg["label_col"], cfg["year_col"],
            *cfg["numerical_cols"], *cfg["categorical_cols"],
        ]
        df = spark.table(cfg["source"]).select(*cols_to_keep)
        if cfg["label_filter_predicate"] is not None:
            df = df.filter(cfg["label_filter_predicate"](col(cfg["label_col"])))
        if sample_fraction is not None:
            df = df.sample(sample_fraction, seed=42)
        df, _added = add_cyclical_features(df, cfg["numerical_cols"])

        # make_splits devuelve los splits ya cacheados y materializados.
        train, val, common_test = make_splits(df, cfg["year_col"])
        eval_splits: dict[str, DataFrame] = {"val": val, "common_test": common_test}
        splits: dict[str, DataFrame] = {"train": train, **eval_splits}

        progress("Materializando splits cacheados...")
        split_counts: dict[str, int] = {}
        for name, split_df in splits.items():
            split_counts[name] = split_df.count()
            progress(f"  {name}: {split_counts[name]:,} filas")
        if split_counts["train"] == 0 or split_counts["val"] == 0:
            print("train o val vacío. ¿Está gold poblado?")
            sys.exit(1)

        # Construimos los Datasets de entrada una vez y los reutilizamos entre runs (barato:
        # el digest es un hash semántico) para que el campo "Datasets used" salga uniforme.
        ds_train = _make_dataset(train, f"{use_case}_train", cfg)
        ds_val = _make_dataset(val, f"{use_case}_val", cfg)
        ds_common_test = _make_dataset(common_test, f"{use_case}_common_test", cfg)
        full_inputs = [(ds_train, "training"), (ds_val, "validation"),
                       (ds_common_test, "testing")]

        common_params = {
            **{f"{k}_count": v for k, v in split_counts.items()},
            "numerical_cols": ",".join(cfg["numerical_cols"]),
            "categorical_cols": ",".join(cfg["categorical_cols"]),
            "phase0_dropped_categoricals": ",".join(redundant),
            "sample_fraction": sample_fraction,
            "nessie_main_hash": nessie_main_hash,
            "source_snapshot_id": str(source_snapshot_id),
        }

        # ---- Baseline -----------------------------------------------------
        print("\n=== Baseline: media(label) por grupo ===")
        progress("Baseline: puntuando el suelo media-por-grupo en cada split...")
        baseline_metrics = log_baseline_run(
            train_df=train,
            eval_splits=splits,
            label_col=cfg["label_col"],
            group_cols=cfg["baseline_group_cols"],
            tags=_common_tags(cfg, phase="baseline",
                              extra={"algorithm": "mean-per-group"}),
            extra_params=common_params,
            input_datasets=full_inputs,
        )
        print(f"  Baseline en common_test: {baseline_metrics}")

        # Única fuente de verdad para elegir al champion; cada fase va añadiendo.
        candidates: list[dict] = []

        def record(run_id, run_name, algorithm, phase, params, all_metrics,
                   model, extra_tags=None):
            candidates.append({
                "run_id": run_id,
                "run_name": run_name,
                "algorithm": algorithm,
                "phase": phase,
                "params": params,
                "metrics": all_metrics,
                "model": model,
                "extra_tags": extra_tags or {},
            })

        # ---- Fase 1: Entrenamiento — CV completo por algoritmo -------------------
        # Un CV(k=cv_folds) por algoritmo sobre su grid completo; gana el algoritmo
        # cuyo bestModel del CV obtiene el menor val_rmse.
        print(f"\n=== Fase 1: Entrenamiento — CV(k={cv_folds}) por algoritmo ===")
        progress(
            f"Fase 1: ajustando {len(ALGORITHMS)} algoritmo(s) "
            f"({', '.join(ALGORITHMS)}) con CV(k={cv_folds})."
        )
        for idx, algorithm in enumerate(ALGORITHMS, start=1):
            grid = full_grids[algorithm]
            run_name = f"train-{algorithm}"
            print(f"\n--- {run_name} ({len(grid)} puntos) ---")
            progress(
                f"Fase 1 [{idx}/{len(ALGORITHMS)}]: iniciando ajuste de "
                f"'{algorithm}' sobre {len(grid)} punto(s) de grid × {cv_folds} folds."
            )
            run_id, best_params, metrics, model = tune_with_cv(
                algorithm=algorithm,
                train_df=train,
                eval_splits=splits,
                label_col=cfg["label_col"],
                pipeline_factory=lambda a, p: _build_pipeline(a, p, cfg),
                param_list=grid,
                num_folds=cv_folds,
                run_name=run_name,
                tags=_common_tags(cfg, phase="training",
                                  extra={"algorithm": algorithm}),
                extra_params=common_params,
                input_datasets=full_inputs,
            )
            record(run_id, run_name, algorithm, "training", best_params,
                   metrics, model)
            _print_metrics(metrics)
            print(f"  mejores params: {best_params}")

        training_winner_candidate = min(
            (c for c in candidates if c["phase"] == "training"),
            key=lambda c: c["metrics"]["val"]["rmse"],
        )
        winner = training_winner_candidate["algorithm"]
        winner_params = training_winner_candidate["params"]
        winner_run_id = training_winner_candidate["run_id"]
        winner_metrics = training_winner_candidate["metrics"]
        progress(
            f"Fase 1 terminada. Ganador por val_rmse: '{winner}' "
            f"(val_rmse={winner_metrics['val']['rmse']:.4f})."
        )
        print(f"\n>>> Ganador Fase 1 (val_rmse): {winner} "
              f"(val_rmse={winner_metrics['val']['rmse']:.4f}, "
              f"common_test_rmse={winner_metrics['common_test']['rmse']:.4f})")
        print(f"    mejores params: {winner_params}")

        # ---- Fase 2: Más datos — A (subconjunto) vs ganador de la Fase 1 (= B) ------
        # Ambos se puntúan sobre common_test = TEST_YEAR, aislando "¿ayudan más datos?".
        print(f"\n=== Fase 2: Más datos — A vs ganador Fase 1 (= B) para {winner} ===")
        MlflowClient().set_tag(winner_run_id, "more_data_subset", "B")
        training_winner_candidate["extra_tags"]["more_data_subset"] = "B"
        print(f"  B = train-{winner} (run {winner_run_id}, "
              f"val_rmse={winner_metrics['val']['rmse']:.4f})")

        sub_train = train.filter(col(cfg["year_col"]).isin(SUBSET_A_YEARS))
        sub_count = sub_train.count()
        run_name = "more-data-A"
        print(f"\n--- {run_name}: años={SUBSET_A_YEARS} ({sub_count:,} filas) ---")
        if sub_count == 0:
            print("  Se omite A: vacío.")
        else:
            progress(
                f"Fase 2: iniciando more-data-A — reentrenando '{winner}' sobre "
                f"años={SUBSET_A_YEARS} ({sub_count:,} filas)."
            )
            ds_train_a = _make_dataset(sub_train, f"{use_case}_train_subsetA", cfg)
            run_id, metrics, model_a = train_and_log_run(
                splits={"train": sub_train, **eval_splits},
                label_col=cfg["label_col"],
                pipeline=_build_pipeline(winner, winner_params, cfg),
                run_name=run_name,
                tags=_common_tags(cfg, phase="more_data",
                                  extra={"algorithm": winner,
                                         "more_data_subset": "A",
                                         "training_data": str(SUBSET_A_YEARS)}),
                extra_params={**common_params, **winner_params,
                              "subset": "A",
                              "subset_train_count": sub_count,
                              "n_train_years": len(SUBSET_A_YEARS)},
                input_datasets=[(ds_train_a, "training"),
                                (ds_val, "validation"),
                                (ds_common_test, "testing")],
            )
            record(run_id, run_name, winner, "more_data",
                   winner_params, metrics, model_a,
                   extra_tags={"more_data_subset": "A",
                               "training_data": str(SUBSET_A_YEARS)})
            _print_metrics(metrics)

        # ---- Resumen — ordenado por val_rmse; common_test se reporta, nunca se usa para rankear ----
        print("\n=== Resumen (todos los candidatos, ordenados por val_rmse) ===")
        header = (
            f"{'run_name':<22} {'algorithm':<6} "
            f"{'val_rmse':>12} {'common_test_rmse':>18} "
            f"{'common_test_mae':>18} {'common_test_r2':>14}"
        )
        print(header)
        print("-" * len(header))
        ranked = sorted(candidates,
                        key=lambda c: c["metrics"]["val"]["rmse"])
        for c in ranked:
            v = c["metrics"]["val"]
            ct = c["metrics"]["common_test"]
            print(
                f"{c['run_name']:<22} {c['algorithm']:<6} "
                f"{v['rmse']:>12.4f} {ct['rmse']:>18.4f} "
                f"{ct['mae']:>18.4f} {ct['r2']:>14.4f}"
            )
        print(
            f"\nBaseline common_test_rmse: {baseline_metrics['rmse']:.4f}  "
            f"(solo informativo — common_test no se usa para rankear candidatos)"
        )

        # ---- Fase 3: Registramos cada candidato ---------------------------
        # El candidato con menor val_rmse es el aspirante a @champion.
        champ_candidate = ranked[0]

        def _build_version_description(c: dict) -> str:
            training_data = c["extra_tags"].get("training_data", str(TRAIN_VAL_YEARS))
            val_m = c["metrics"]["val"]
            ct_m = c["metrics"]["common_test"]
            return (
                f"Run name: {c['run_name']}. "
                f"Algorithm: {c['algorithm']}. Phase: {c['phase']}.\n"
                f"Params: {c['params']}.\n"
                f"Source: {cfg['source']} (Nessie main "
                f"hash={nessie_main_hash[:12] or 'unknown'}, "
                f"snapshot_id={source_snapshot_id}). "
                f"Target: {cfg['label_col']}.\n"
                f"Training data: {training_data}.\n"
                f"Numerical features (base): {', '.join(cfg['numerical_cols'])}.\n"
                f"Categorical features: {', '.join(cfg['categorical_cols'])}.\n"
                f"Scores: val_rmse={val_m['rmse']:.4f}, "
                f"common_test_rmse={ct_m['rmse']:.4f}, "
                f"common_test_mae={ct_m['mae']:.4f}, "
                f"common_test_r2={ct_m['r2']:.4f}. "
                f"Baseline common_test_rmse={baseline_metrics['rmse']:.4f}."
            )

        def _build_version_tags(c: dict) -> dict[str, str]:
            training_data = c["extra_tags"].get("training_data", str(TRAIN_VAL_YEARS))
            val_m = c["metrics"]["val"]
            ct_m = c["metrics"]["common_test"]
            tags = {
                "training_data": training_data,
                "algorithm": c["algorithm"],
                "phase": c["phase"],
                "run_name": c["run_name"],
                "rmse": f"{ct_m['rmse']:.4f}",
                "mae":  f"{ct_m['mae']:.4f}",
                "val_rmse":         f"{val_m['rmse']:.4f}",
                "common_test_rmse": f"{ct_m['rmse']:.4f}",
                "common_test_mae":  f"{ct_m['mae']:.4f}",
                "common_test_r2":   f"{ct_m['r2']:.4f}",
                "baseline_common_test_rmse": f"{baseline_metrics['rmse']:.4f}",
                "nessie_main_hash":   nessie_main_hash,
                "source_snapshot_id": str(source_snapshot_id),
            }
            if "more_data_subset" in c["extra_tags"]:
                tags["more_data_subset"] = c["extra_tags"]["more_data_subset"]
            return tags

        model_description = (
            f"Spark MLlib regression for {cfg['experiment']}. "
            f"Source: {cfg['source']} (Nessie main, gold layer). "
            f"Target: {cfg['label_col']}. "
            f"Each src/ml/train.py {use_case} invocation registers every "
            f"candidate it trains (3 Phase-1 Training + 1 Phase-2 "
            f"More-data = 4 per run). The best across phases (lowest "
            f"val_rmse) gets @champion only if its val_rmse is strictly "
            f"below the previous champion's val_rmse; otherwise it stays "
            f"validation_status=\"challenger\". common_test is held out and "
            f"never used to rank candidates — the test metrics on each "
            f"version are the unbiased generalization estimate. All other "
            f"candidates from the run are tagged "
            f"validation_status=\"challenger\". @challenger alias points to "
            f"the best non-champion version from the most recent run. "
            f"Every version tags the Nessie main commit hash and the source "
            f"Iceberg snapshot_id used at training time (reproducibility). "
            f"The new @champion also gets an inference_latency_ms_per_row "
            f"tag from a 1000-row sample of common_test. The registry is "
            f"pruned to the {REGISTRY_MAX_VERSIONS} most-recent versions "
            f"after each run; aliased versions are always retained."
        )
        registered_tags = {
            "framework": "spark-mllib",
            "task": "regression",
            "project_name": cfg["project_name"],
            "created_by": CREATED_BY,
        }

        progress(
            f"Fase 3: registrando {len(candidates)} candidatos en "
            f"{cfg['experiment']}..."
        )
        registered = register_candidates(
            name=cfg["experiment"],
            candidates=candidates,
            champion_run_id=champ_candidate["run_id"],
            promotion_metric="val_rmse",
            build_version_description=_build_version_description,
            build_version_tags=_build_version_tags,
            registered_model_description=model_description,
            registered_model_tags=registered_tags,
            latency_model=champ_candidate["model"],
            latency_sample_df=common_test,
        )

        print(
            f"\n=== Fase 3: registradas {len(registered)} versión(es) de "
            f"{cfg['experiment']} (ordenadas por val_rmse) ==="
        )
        for c in ranked:
            mv, status = registered[c["run_id"]]
            print(
                f"  v{mv.version:<3}  {c['run_name']:<22}  "
                f"val_rmse={c['metrics']['val']['rmse']:.4f}  "
                f"common_test_rmse={c['metrics']['common_test']['rmse']:.4f}  "
                f"[{status}]"
            )
        print(
            f"\nCargar champion: "
            f"mlflow.spark.load_model('models:/{cfg['experiment']}@champion')"
        )
        print(f"URI de tracking: {MLFLOW_TRACKING_URI}  Experimento: {cfg['experiment']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
