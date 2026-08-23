"""Script de entrenamiento unificado: spark-submit src/ml/train.py <fare|duration|demand> [--fast].
Cada ejecución registra todo en un único experimento de MLflow:
  Fase 0    descarta las categóricas redundantes (p. ej. borough, derivable de location_id).
  Fase 1    un CV por algoritmo sobre su grid completo, entrenando siempre con
            TRAIN_VAL_YEARS; gana el menor val_rmse.
  Fase 2    registra los 3 candidatos; el ganador se queda con @champion solo si su
            val_rmse mejora estrictamente el del champion del registro, que entonces
            pasa a @previous (punto de reversión).
"""

import sys

import mlflow
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
    TRAIN_VAL_YEARS,
    add_cyclical_features,
    make_splits,
    resolve_numerical_cols,
)
from ml.tracking import (
    MLFLOW_TRACKING_URI,
    get_ml_spark,
    progress,
    register_candidates,
    setup_mlflow,
    tune_with_cv,
)


CREATED_BY = "jgalan"

# Conjunto de features de yellow_taxi_trips, compartido por los casos fare y duration.
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
        "label_filter_predicate": None,
        "redundant_categoricals": TRIP_REDUNDANT_CATEGORICALS,
    },
    "duration": {
        "experiment": "trip_duration_prediction",
        "app_name": "ml-trip-duration",
        "source": "nessie.nyc.yellow_taxi_trips",
        "label_col": "trip_duration_minutes",
        "year_col": "pickup_year",
        "numerical_cols": TRIP_NUMERICAL_COLS,
        "categorical_cols": TRIP_CATEGORICAL_COLS,
        "glr_family": "gamma",
        "project_name": "NYC Taxi Trip Duration Prediction",
        "label_filter_predicate": lambda c: c > 0,
        "redundant_categoricals": TRIP_REDUNDANT_CATEGORICALS,
    },
    "demand": {
        "experiment": "demand_forecast",
        "app_name": "ml-demand-forecast",
        "source": "nessie.nyc.pickup_demand_zone_hour",
        "label_col": "n_pickups",
        "year_col": "hour_year",
        "numerical_cols": [
            "hour_month", "hour_week",
            "hour_dow", "hour_of_day",
        ],
        "categorical_cols": [
            "pu_location_id", "pu_borough", "pu_service_zone",
        ],
        "glr_family": "poisson",      # recuentos enteros no negativos
        "project_name": "NYC Taxi Pickup Demand Forecast",
        "label_filter_predicate": None,
        "redundant_categoricals": ["pu_borough", "pu_service_zone"],
    },
}


def _parse_cli(argv: list[str]) -> tuple[str, bool]:
    """Devuelve (use_case, fast)"""
    positional = [a for a in argv if not a.startswith("--")]
    if len(positional) != 1 or positional[0] not in USE_CASES:
        names = "|".join(USE_CASES)
        print(f"Uso: spark-submit src/ml/train.py <{names}> [--fast]")
        sys.exit(2)
    return positional[0], fast_mode_enabled(argv)


def _build_pipeline(algorithm: str, params: dict, cfg: dict):
    """Construye el pipeline de features y el estimador para `algorithm` según cfg."""
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
    """Crea un split como Dataset de MLflow. El digest es el hash semántico del plan
    lógico (sin escaneo). Sin `version=` — eso es solo para Delta; el snapshot se fija
    con source_snapshot_id."""
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
        elif sample_fraction >= 1.0:
            progress(
                f"Entrenando sobre la tabla gold completa "
                f"(sample_fraction={SAMPLE_FRACTION:.2f}; bajarla es la palanca "
                "para acotar el coste del CV)."
            )
        else:
            progress(
                f"Muestreando {SAMPLE_FRACTION:.0%} de gold para entrenar "
                "(acota el coste del CV; cada fold sigue con millones de filas)."
            )

        # ---- Fase 0: se descartan las categóricas redundantes (derivables de location_id) ----
        redundant = list(cfg["redundant_categoricals"])
        cfg = {**cfg, "categorical_cols": [
            c for c in cfg["categorical_cols"] if c not in redundant
        ]}
        print("\n=== Fase 0: descartando categóricas redundantes ===")
        print(f"  descartadas: {redundant}")
        print(f"  categóricas restantes: {cfg['categorical_cols']}")

        # ---- Carga, proyección, muestreo opcional y features cíclicas ----
        # El estado de los datos (hash de main en Nessie y snapshot id del origen) se fija
        # en cada versión para poder reconstruir después la entrada exacta.
        nessie_main_hash = nessie.current_ref_hash(spark, "main") or ""
        source_snapshot_id = nessie.latest_snapshot_id(spark, cfg["source"])
        progress(
            f"Estado de datos fijado: nessie main={nessie_main_hash[:12] or 'unknown'}, "
            f"{cfg['source']} snapshot_id={source_snapshot_id}"
        )

        progress(f"Leyendo {cfg['source']}...")
        cols_to_keep = [
            cfg["label_col"], cfg["year_col"],
            *cfg["numerical_cols"], *cfg["categorical_cols"],
        ]
        df = spark.table(cfg["source"]).select(*cols_to_keep)
        if cfg["label_filter_predicate"] is not None:
            df = df.filter(cfg["label_filter_predicate"](col(cfg["label_col"])))
        if sample_fraction < 1.0:
            df = df.sample(sample_fraction, seed=42)
        df = add_cyclical_features(df, cfg["numerical_cols"])

        # make_splits devuelve los splits ya cacheados
        train, val, common_test = make_splits(df, cfg["year_col"])
        splits: dict[str, DataFrame] = {
            "train": train, "val": val, "common_test": common_test,
        }

        progress("Materializando splits cacheados...")
        split_counts: dict[str, int] = {}
        for name, split_df in splits.items():
            split_counts[name] = split_df.count()
            progress(f"  {name}: {split_counts[name]:,} filas")
        if split_counts["train"] == 0 or split_counts["val"] == 0:
            print("train o val vacío. ¿Está gold poblado?")
            sys.exit(1)

        full_inputs = [
            (_make_dataset(train, f"{use_case}_train", cfg), "training"),
            (_make_dataset(val, f"{use_case}_val", cfg), "validation"),
            (_make_dataset(common_test, f"{use_case}_common_test", cfg), "testing"),
        ]

        common_params = {
            **{f"{k}_count": v for k, v in split_counts.items()},
            "numerical_cols": ",".join(cfg["numerical_cols"]),
            "categorical_cols": ",".join(cfg["categorical_cols"]),
            "phase0_dropped_categoricals": ",".join(redundant),
            "sample_fraction": sample_fraction,
            "nessie_main_hash": nessie_main_hash,
            "source_snapshot_id": str(source_snapshot_id),
        }

        # ---- Fase 1: Entrenamiento — CV completo por algoritmo -------------------
        # Un CV(k=cv_folds) por algoritmo sobre su grid completo; gana el algoritmo
        # cuyo bestModel del CV obtiene el menor val_rmse. Todos entrenan sobre el
        # mismo train (TRAIN_VAL_YEARS).
        candidates: list[dict] = []
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
            candidates.append({
                "run_id": run_id,
                "run_name": run_name,
                "algorithm": algorithm,
                "phase": "training",
                "params": best_params,
                "metrics": metrics,
                "model": model,
            })
            _print_metrics(metrics)
            print(f"  mejores params: {best_params}")

        ranked = sorted(candidates, key=lambda c: c["metrics"]["val"]["rmse"])
        champ_candidate = ranked[0]
        progress(
            f"Fase 1 terminada. Ganador por val_rmse: "
            f"'{champ_candidate['algorithm']}' "
            f"(val_rmse={champ_candidate['metrics']['val']['rmse']:.4f}, "
            f"params={champ_candidate['params']})."
        )

        # ---- Resumen — ordenado por val_rmse----
        print("\n=== Resumen (todos los candidatos, ordenados por val_rmse) ===")
        header = (
            f"{'run_name':<14} {'algorithm':<6} "
            f"{'val_rmse':>12} {'common_test_rmse':>18} "
            f"{'common_test_mae':>18} {'common_test_r2':>14}"
        )
        print(header)
        print("-" * len(header))
        for c in ranked:
            v = c["metrics"]["val"]
            ct = c["metrics"]["common_test"]
            print(
                f"{c['run_name']:<14} {c['algorithm']:<6} "
                f"{v['rmse']:>12.4f} {ct['rmse']:>18.4f} "
                f"{ct['mae']:>18.4f} {ct['r2']:>14.4f}"
            )
        print(
            "\n(common_test es solo informativo — no se usa para ordenar candidatos)"
        )

        # ---- Fase 2: se registra cada candidato ---------------------------
        # champ_candidate (el de menor val_rmse) es el aspirante a @champion.
        train_years = " y ".join(str(y) for y in TRAIN_VAL_YEARS)

        # La descripción contiene los hiperparámetros y el conjunto de predictores.
        def _build_version_description(c: dict) -> str:
            ct_m = c["metrics"]["common_test"]
            return (
                f"{c['algorithm'].upper()} para predecir {cfg['label_col']} "
                f"a partir de {cfg['source']}. Entrenado con {train_years}.\n"
                f"Hiperparámetros: {c['params']}.\n"
                f"Predictores numéricos (base): {', '.join(cfg['numerical_cols'])}.\n"
                f"Predictores categóricos: {', '.join(cfg['categorical_cols'])}.\n"
                f"val_rmse={c['metrics']['val']['rmse']:.4f} · "
                f"common_test: rmse={ct_m['rmse']:.4f} mae={ct_m['mae']:.4f} "
                f"r2={ct_m['r2']:.4f}."
            )

        def _build_version_tags(c: dict) -> dict[str, str]:
            val_m = c["metrics"]["val"]
            ct_m = c["metrics"]["common_test"]
            return {
                "training_data": str(TRAIN_VAL_YEARS),
                "algorithm": c["algorithm"],
                "phase": c["phase"],
                "run_name": c["run_name"],
                "rmse": f"{ct_m['rmse']:.4f}",
                "mae":  f"{ct_m['mae']:.4f}",
                "val_rmse":         f"{val_m['rmse']:.4f}",
                "common_test_rmse": f"{ct_m['rmse']:.4f}",
                "common_test_mae":  f"{ct_m['mae']:.4f}",
                "common_test_r2":   f"{ct_m['r2']:.4f}",
                "nessie_main_hash":   nessie_main_hash,
                "source_snapshot_id": str(source_snapshot_id),
            }

        model_description = (
            f"Regresión con Spark MLlib para predecir {cfg['label_col']} "
            f"a partir de {cfg['source']} (capa gold), entrenada con {train_years}.\n"
            f"@champion es la versión vigente (menor val_rmse); "
            f"@previous, la que relevó y a la que se revertiría."
        )
        registered_tags = {
            "framework": "spark-mllib",
            "task": "regression",
            "project_name": cfg["project_name"],
            "created_by": CREATED_BY,
        }

        progress(
            f"Fase 2: registrando {len(candidates)} candidatos en "
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
            f"\n=== Fase 2: registradas {len(registered)} versión(es) de "
            f"{cfg['experiment']} (ordenadas por val_rmse) ==="
        )
        for c in ranked:
            mv, status = registered[c["run_id"]]
            print(
                f"  v{mv.version:<3}  {c['run_name']:<14}  "
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
