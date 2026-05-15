"""ML use case 2 — predict trip_duration_minutes from gold trip features.

Reads only nessie.gold.yellow_taxi_trips on main. Post-trip explanatory model
(uses measured trip_distance), not a real-time ETA. Three-phase workflow per
script run, all logged to the same MLflow experiment with `phase` tag:

  Phase 1 — algorithmrithm comparison: LR, RF, GBT with default hyperparameters.
  Phase 2 — hyperparameter tuning of the algorithmrithm with lowest val RMSE.
  Phase 3 — learning curve over cumulative year subsets with the tuned model.
"""

import sys

from pyspark.sql.functions import col

from ml.common import (
    DEFAULT_PARAMS,
    MLFLOW_TRACKING_URI,
    PARAM_GRIDS,
    TEST_YEARS,
    TRAIN_YEAR_SUBSETS,
    algorithm_pipeline_config,
    build_regression_pipeline,
    get_ml_spark,
    make_estimator,
    make_splits,
    register_final_model,
    setup_mlflow,
    train_and_log_run,
)

EXPERIMENT = "trip_duration"
SOURCE = "nessie.gold.yellow_taxi_trips"
LABEL_COL = "trip_duration_minutes"
YEAR_COL = "pickup_year"

# Same pre-trip-style feature set as fare_prediction. fare_amount and
# total_amount are excluded — both contain duration information (the meter
# charges per minute) so using them would be circular.
NUMERICAL_COLS = [
    "trip_distance", "passenger_count",
    "pickup_year", "pickup_month", "pickup_week", "pickup_dow", "pickup_hour",
]
CATEGORICAL_COLS = [
    "pu_location_id", "do_location_id",
    "pu_borough", "do_borough",
    "vendor_id", "ratecode_id", "payment_type",
]
ALGORITHMS = ("lr", "rf", "gbt")


def _build_pipeline(algorithm: str, params: dict):
    return build_regression_pipeline(
        numerical_cols=NUMERICAL_COLS,
        categorical_cols=CATEGORICAL_COLS,
        label_col=LABEL_COL,
        estimator=make_estimator(algorithm, **params),
        **algorithm_pipeline_config(algorithm),
    )


def _print_metrics(metrics: dict[str, dict[str, float]]) -> None:
    for split, m in metrics.items():
        parts = "  ".join(f"{n}={v:.4f}" for n, v in m.items())
        print(f"  {split:<10} {parts}")


def main() -> None:
    spark = get_ml_spark(app_name="ml-trip-duration")
    try:
        setup_mlflow(EXPERIMENT)

        # Silver enforces pickup < dropoff but unix_timestamp truncates to the
        # second, so sub-second trips render as duration 0. Drop them.
        df = spark.table(SOURCE).filter(col(LABEL_COL) > 0)

        train, val, test_id, test_ood = make_splits(df, YEAR_COL, TEST_YEARS)
        train, val, test_id, test_ood = (
            train.cache(), val.cache(), test_id.cache(), test_ood.cache(),
        )
        splits = {"train": train, "val": val, "test_id": test_id, "test_ood": test_ood}
        split_counts = {k: v.count() for k, v in splits.items()}
        print("\nSplit sizes:")
        for k, v in split_counts.items():
            print(f"  {k:<10} {v:>12,} rows")
        if split_counts["train"] == 0 or split_counts["val"] == 0:
            print("Empty train or val. Is gold populated?")
            sys.exit(1)

        common_tags = {
            "source_table": SOURCE,
            "label": LABEL_COL,
            "year_col": YEAR_COL,
            "test_ood_years": str(TEST_YEARS),
        }
        common_params = {
            **{f"{k}_count": v for k, v in split_counts.items()},
            "numerical_cols": ",".join(NUMERICAL_COLS),
            "categorical_cols": ",".join(CATEGORICAL_COLS),
        }

        # ============ Phase 1: Algorithm comparison ============
        print("\n=== Phase 1: Algorithm comparison ===")
        val_rmse: dict[str, float] = {}
        for algorithm in ALGORITHMS:
            print(f"\n--- {algorithm} ---")
            params = DEFAULT_PARAMS[algorithm]
            _, metrics = train_and_log_run(
                splits=splits,
                label_col=LABEL_COL,
                pipeline=_build_pipeline(algorithm, params),
                run_name=f"compare-{algorithm}",
                tags={**common_tags, "phase": "compare", "algorithm": algorithm},
                extra_params={**common_params, **params},
            )
            val_rmse[algorithm] = metrics["val"]["rmse"]
            _print_metrics(metrics)

        winner = min(val_rmse, key=val_rmse.get)
        print(f"\n>>> Phase 1 winner: {winner} (val RMSE = {val_rmse[winner]:.4f})")

        # ============ Phase 2: Hyperparameter tuning ============
        print(f"\n=== Phase 2: Tuning {winner} ===")
        best_params: dict | None = None
        best_val_rmse = float("inf")
        for i, params in enumerate(PARAM_GRIDS[winner]):
            print(f"\n--- {winner}-grid-{i}: {params} ---")
            _, metrics = train_and_log_run(
                splits=splits,
                label_col=LABEL_COL,
                pipeline=_build_pipeline(winner, params),
                run_name=f"tune-{winner}-{i}",
                tags={**common_tags, "phase": "tune", "algorithm": winner},
                extra_params={**common_params, **params, "grid_idx": i},
            )
            if metrics["val"]["rmse"] < best_val_rmse:
                best_val_rmse = metrics["val"]["rmse"]
                best_params = params
            _print_metrics(metrics)

        assert best_params is not None
        print(f"\n>>> Phase 2 best params: {best_params} (val RMSE = {best_val_rmse:.4f})")

        # ============ Phase 3: Year-subset learning curve ============
        print(f"\n=== Phase 3: Learning curve with tuned {winner} ===")
        curve: list[dict] = []
        last_run_id: str | None = None
        last_metrics: dict[str, dict[str, float]] | None = None
        for train_years in TRAIN_YEAR_SUBSETS:
            print(f"\n--- Train years: {train_years} ---")
            sub_train = train.filter(train[YEAR_COL].isin(train_years))
            sub_train_count = sub_train.count()
            print(f"  train subset: {sub_train_count:,} rows")
            if sub_train_count == 0:
                print(f"  Skipping {train_years}: empty after filter.")
                continue

            sub_splits = {**splits, "train": sub_train}
            run_id, metrics = train_and_log_run(
                splits=sub_splits,
                label_col=LABEL_COL,
                pipeline=_build_pipeline(winner, best_params),
                run_name=f"subset-up-to-{train_years[-1]}",
                tags={
                    **common_tags,
                    "phase": "subset",
                    "algorithm": winner,
                    "train_years": str(train_years),
                },
                extra_params={
                    **common_params,
                    **best_params,
                    "train_count": sub_train_count,
                    "n_train_years": len(train_years),
                },
            )
            last_run_id, last_metrics = run_id, metrics
            curve.append({
                "train_years": train_years,
                **{f"{s}_rmse": metrics[s]["rmse"] for s in metrics},
            })
            _print_metrics(metrics)

        # Final summary
        print("\n=== Summary (learning curve, RMSE per split) ===")
        header = f"{'train_years':<28} {'val':>10} {'test_id':>10} {'test_ood':>10}"
        print(header)
        print("-" * len(header))
        for r in curve:
            print(
                f"{str(r['train_years']):<28} "
                f"{r['val_rmse']:>10.4f} {r['test_id_rmse']:>10.4f} {r['test_ood_rmse']:>10.4f}"
            )

        # ============ Register final model in MLflow Model Registry ============
        # Champion / challenger pattern: new version is registered every time;
        # if it beats the current @champion on val_rmse it gets promoted, else
        # it stays as @challenger.
        if last_run_id is not None and last_metrics is not None:
            version_description = (
                f"Algo: {winner}. Params: {best_params}. "
                f"Train years: {TRAIN_YEAR_SUBSETS[-1]}.\n"
                f"Source: {SOURCE} (Nessie main). Target: {LABEL_COL} (filtered > 0).\n"
                f"Numerical features: {', '.join(NUMERICAL_COLS)}.\n"
                f"Categorical features: {', '.join(CATEGORICAL_COLS)}.\n"
                f"Scores: "
                f"val_rmse={last_metrics['val']['rmse']:.4f}, "
                f"test_id_rmse={last_metrics['test_id']['rmse']:.4f}, "
                f"test_ood_rmse={last_metrics['test_ood']['rmse']:.4f}."
            )
            model_description = (
                f"Spark MLlib regression for {EXPERIMENT}. "
                f"Source: {SOURCE} (Nessie main). Target: {LABEL_COL} "
                f"(rows with duration <= 0 are filtered out — sub-second "
                f"trips render as 0 due to unix_timestamp truncation). "
                f"Each version is the best model from one run of "
                f"src/ml/trip_duration.py — algorithmrithm comparison → "
                f"hyperparameter tuning → year-subset learning curve. "
                f"Promotion is Pareto-style: @champion points to the best "
                f"version so far on val_rmse (primary, strict improvement) "
                f"that doesn't degrade test_id_rmse beyond 2% (guard)."
            )
            registered_tags = {
                "project": "NYC Taxi Trip Duration Prediction",
                "task": "regression",
                "framework": "spark-mllib",
                "layer": SOURCE,
            }
            version_tags_dict = {
                "algorithm": winner,
                "n_train_years": str(len(TRAIN_YEAR_SUBSETS[-1])),
                "train_years": str(TRAIN_YEAR_SUBSETS[-1]),
                "val_rmse":      f"{last_metrics['val']['rmse']:.4f}",
                "val_mae":       f"{last_metrics['val']['mae']:.4f}",
                "val_r2":        f"{last_metrics['val']['r2']:.4f}",
                "test_id_rmse":  f"{last_metrics['test_id']['rmse']:.4f}",
                "test_id_mae":   f"{last_metrics['test_id']['mae']:.4f}",
                "test_id_r2":    f"{last_metrics['test_id']['r2']:.4f}",
                "test_ood_rmse": f"{last_metrics['test_ood']['rmse']:.4f}",
                "test_ood_mae":  f"{last_metrics['test_ood']['mae']:.4f}",
                "test_ood_r2":   f"{last_metrics['test_ood']['r2']:.4f}",
            }
            mv, alias, _promoted = register_final_model(
                run_id=last_run_id,
                name=EXPERIMENT,
                version_description=version_description,
                version_tags=version_tags_dict,
                registered_model_description=model_description,
                registered_model_tags=registered_tags,
                promotion_metric="val_rmse",
                promotion_guard_metric="test_id_rmse",
                promotion_guard_tolerance=0.02,
            )
            print(f"\nRegistered {EXPERIMENT} v{mv.version} as @{alias}")
            print(f"  Load with: mlflow.spark.load_model('models:/{EXPERIMENT}@{alias}')")

        print(f"\nTracking URI: {MLFLOW_TRACKING_URI}  Experiment: {EXPERIMENT}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
