"""Unified ML training script — one entry point for all three use cases.

  spark-submit src/ml/train.py <use_case> [--fast]

  use_case ∈ {fare, duration, demand}

Phases (run end-to-end per invocation, all logged to one MLflow experiment):

  Phase 0 — Drop redundant categoricals (e.g. borough — derivable from
            location_id). Applied to the feature set up-front, so every
            subsequent phase trains on the reduced columns.
  baseline      — mean(label) per (group_cols). Floor reference; mean is
                  the RMSE-optimal constant-per-group predictor (median
                  would be MAE-optimal — wrong loss for our metric).
  Phase 1 — Training      : one CV(k=3) per algorithm over its full grid
                            (5-pt for GLR, 8-pt for GBT, 7-pt for RFR). The
                            within-run winner is the algorithm whose
                            CV-bestModel scores lowest on val_rmse. Every
                            algorithm gets registered with its full-grid
                            best, so the "losing" algorithms still appear
                            in the registry as fairly-tuned challengers.
  Phase 2 — More-data     : winner refit on subset A (SUBSET_A_YEARS — the
                            two earliest training years) vs subset B (all
                            of TRAIN_VAL_YEARS, reused as the Phase-1
                            winner without refit). Both scored on the same
                            common_test = TEST_YEAR, so neither sees the
                            held-out year — the contrast isolates "does
                            more training data help?".
  Phase 3 — Register every candidate (3 Phase-1 Training + 1 Phase-2
            More-data = 4 per run) as a new MLflow model version. The
            within-run winner (lowest val_rmse) gets @champion only if its
            val_rmse is strictly below the previous registry champion's
            val_rmse; otherwise it stays validation_status="challenger".
            All other candidates from the run are tagged
            validation_status="challenger". The @challenger alias points to
            the best non-champion version from this run.

Metric policy: val_rmse drives every selection decision (within-run
@champion contender, cross-run @champion promotion). common_test is held
out — its metrics are logged on every version but never used to rank
candidates, so the diagnostic notebook can report them as an unbiased
generalization estimate.

Split policy: last-year holdout. common_test is every row of TEST_YEAR
(2025) — the model has never seen any of it during training or selection.
train + val are stratified-by-year random split of TRAIN_VAL_YEARS
(2019/2020/2022/2023). This closes the within-year temporal-autocorrelation
leak of a per-year random split: no two trips minutes apart end up across
the train/test boundary.

Reproducibility policy: every registered version carries the Nessie main
commit hash and the source Iceberg snapshot_id as tags
(`nessie_main_hash`, `source_snapshot_id`). The full hash pins every
table on every branch as of training time, so a model card is enough to
rebuild the exact training input later — even if gold has rolled forward.

Latency policy: on promotion the new @champion is scored on 1000 rows of
common_test and the elapsed time is logged on its version
(`inference_latency_ms_per_row`, `inference_latency_sample_n`,
`inference_latency_total_s`). The numbers are Spark-cluster latency, not
a production SLO — they're a regression check between champions ("did v40
suddenly get 5× slower than v39?").

Registry retention policy: at the end of every run the registry is
pruned to the top REGISTRY_MAX_VERSIONS most-recent versions, with the
exception that any version carrying an @champion or @challenger alias is
always kept. Caps registry growth (~5 versions per train.py run) without
losing the audit trail to whatever the current aliases point at.
"""

import sys

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

# yellow_taxi_trips feature set — shared by fare and duration use cases.
TRIP_NUMERICAL_COLS = [
    "trip_distance", "passenger_count",
    "pickup_year", "pickup_month", "pickup_week",
    "pickup_dow", "pickup_hour",
]
TRIP_CATEGORICAL_COLS = [
    "pu_location_id", "do_location_id",
    "pu_borough", "do_borough",
    "vendor_id", "ratecode_id", "payment_type",
]
TRIP_BASELINE_GROUP_COLS = ["pu_location_id", "pickup_hour"]
# Phase 0: borough is derivable from location_id, so it carries no information
# beyond the ID itself. Dropped up-front for every algorithm/phase.
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
        "glr_family": "gamma",        # positive, right-skewed
        "project_name": "NYC Taxi Fare Prediction",
        "baseline_group_cols": TRIP_BASELINE_GROUP_COLS,
        # Silver enforces fare > 0; no filter needed here.
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
        # Sub-second trips: unix_timestamp truncates → duration 0. Filter so
        # GLR (gamma family requires y > 0) does not blow up.
        "label_filter_predicate": lambda c: c > 0,
        "redundant_categoricals": TRIP_REDUNDANT_CATEGORICALS,
    },
    "demand": {
        "experiment": "demand_forecast",
        "app_name": "ml-demand-forecast",
        "source": "nessie.nyc.pickup_demand_zone_hour",
        "label_col": "n_pickups",
        "year_col": "hour_year",
        # Calendar features only. Aggregated columns (avg_fare, total_revenue,
        # ...) require n_pickups to exist, so they would be leakage.
        "numerical_cols": [
            "hour_year", "hour_month", "hour_week",
            "hour_dow", "hour_of_day",
        ],
        "categorical_cols": [
            "pu_location_id", "pu_borough", "pu_service_zone",
        ],
        "glr_family": "poisson",      # non-negative integer counts
        "project_name": "NYC Taxi Pickup Demand Forecast",
        "baseline_group_cols": ["pu_location_id", "hour_of_day"],
        "label_filter_predicate": None,
        # Phase 0: pu_borough and pu_service_zone are both derivable from
        # pu_location_id via the zone lookup — drop both.
        "redundant_categoricals": ["pu_borough", "pu_service_zone"],
    },
}


def _parse_cli(argv: list[str]) -> tuple[str, bool]:
    """Returns (use_case, fast). Exits with a usage message on bad input."""
    positional = [a for a in argv if not a.startswith("--")]
    if len(positional) != 1 or positional[0] not in USE_CASES:
        names = "|".join(USE_CASES)
        print(f"Usage: spark-submit src/ml/train.py <{names}> [--fast]")
        sys.exit(2)
    return positional[0], fast_mode_enabled(argv)


def _build_pipeline(algorithm: str, params: dict, cfg: dict):
    """Compose feature pipeline + estimator for `algorithm` under cfg."""
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


def _common_tags(cfg: dict, phase: str, extra: dict | None = None) -> dict:
    tags = {
        "source_table": cfg["source"],
        "label": cfg["label_col"],
        "year_col": cfg["year_col"],
        "phase": phase,
        "project_name": cfg["project_name"],
        "created_by": CREATED_BY,
    }
    if extra:
        tags.update(extra)
    return tags


def main() -> None:
    use_case, fast = _parse_cli(sys.argv[1:])
    cfg = USE_CASES[use_case]
    sample_fraction = FAST_SAMPLE_FRACTION if fast else None
    full_grids = FAST_FULL_PARAM_GRIDS if fast else FULL_PARAM_GRIDS
    cv_folds = FAST_CV_NUM_FOLDS if fast else CV_NUM_FOLDS

    progress(f"Starting Spark for use_case={use_case!r} fast={fast}...")
    spark = get_ml_spark(app_name=cfg["app_name"])
    try:
        progress("Spark ready. Configuring MLflow...")
        setup_mlflow(cfg["experiment"])
        if fast:
            progress(
                f"FAST MODE — sampling {FAST_SAMPLE_FRACTION:.0%} of gold, "
                "tiny grids/folds. Smoke test only; metrics not meaningful."
            )

        # ---- Phase 0: drop redundant categoricals ------------------------
        # Borough (and pu_service_zone for demand) are derivable from
        # location_id via the zone lookup, so they carry no information
        # beyond it. Drop up-front so every algorithm/phase trains on the
        # same reduced feature set.
        redundant = list(cfg["redundant_categoricals"])
        cfg = {**cfg, "categorical_cols": [
            c for c in cfg["categorical_cols"] if c not in redundant
        ]}
        print("\n=== Phase 0: dropping redundant categoricals ===")
        print(f"  dropped: {redundant}")
        print(f"  remaining categoricals: {cfg['categorical_cols']}")

        # ---- Load + project + optional sample + cyclical features ----
        # Pin the data state at run-time: Nessie main hash + Iceberg snapshot
        # id of the source table. These travel onto every registered version
        # as tags so a future reader can rebuild the exact training input.
        nessie_main_hash = nessie.current_ref_hash(spark, "main") or ""
        source_snapshot_id = nessie.latest_snapshot_id(spark, cfg["source"])
        progress(
            f"Pinned data state: nessie main={nessie_main_hash[:12] or 'unknown'}, "
            f"{cfg['source']} snapshot_id={source_snapshot_id}"
        )

        progress(f"Reading {cfg['source']}...")
        cols_to_keep = [cfg["label_col"], *cfg["numerical_cols"], *cfg["categorical_cols"]]
        df = spark.table(cfg["source"]).select(*cols_to_keep)
        if cfg["label_filter_predicate"] is not None:
            df = df.filter(cfg["label_filter_predicate"](col(cfg["label_col"])))
        if sample_fraction is not None:
            df = df.sample(sample_fraction, seed=42)
        df, _added = add_cyclical_features(df, cfg["numerical_cols"])

        train, val, common_test = make_splits(df, cfg["year_col"])
        train, val, common_test = train.cache(), val.cache(), common_test.cache()
        eval_splits: dict[str, DataFrame] = {"val": val, "common_test": common_test}
        splits: dict[str, DataFrame] = {"train": train, **eval_splits}

        progress("Materializing cached splits...")
        split_counts: dict[str, int] = {}
        for name, split_df in splits.items():
            split_counts[name] = split_df.count()
            progress(f"  {name}: {split_counts[name]:,} rows")
        if split_counts["train"] == 0 or split_counts["val"] == 0:
            print("Empty train or val. Is gold populated?")
            sys.exit(1)

        common_params = {
            **{f"{k}_count": v for k, v in split_counts.items()},
            "numerical_cols": ",".join(cfg["numerical_cols"]),
            "categorical_cols": ",".join(cfg["categorical_cols"]),
            "phase0_dropped_categoricals": ",".join(redundant),
            "training_data": str(TRAIN_VAL_YEARS),
            "nessie_main_hash": nessie_main_hash,
            "source_snapshot_id": str(source_snapshot_id),
        }

        # ---- Baseline -----------------------------------------------------
        print("\n=== Baseline: mean(label) per group ===")
        baseline_metrics = log_baseline_run(
            train_df=train,
            test_df=common_test,
            label_col=cfg["label_col"],
            group_cols=cfg["baseline_group_cols"],
            tags=_common_tags(cfg, phase="baseline",
                              extra={"algorithm": "mean-per-group"}),
            extra_params=common_params,
        )
        print(f"  Baseline on common_test: {baseline_metrics}")

        # candidates: single source of truth for within-run champion
        # selection. Each phase appends; winners are picked by filtering on
        # `phase` and the relevant split metric.
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

        # ---- Phase 1: Training — full CV per algorithm -------------------
        # One CV(k=cv_folds) per algorithm over its FULL grid. The within-
        # run winner is whichever algorithm's CV-bestModel scores lowest on
        # val_rmse. Every algorithm gets registered with its fairly-tuned
        # best, so the losing algorithms still appear in the registry as
        # challengers (not as 3-pt screening points like the old Phase 1
        # design). Collapses the previous Phase 1 (screening) + Phase 2
        # (winner-only optimization) into a single training step.
        print(f"\n=== Phase 1: Training — CV(k={cv_folds}) per algorithm ===")
        for algorithm in ALGORITHMS:
            grid = full_grids[algorithm]
            run_name = f"train-{algorithm}"
            print(f"\n--- {run_name} ({len(grid)} points) ---")
            run_id, best_params, metrics, model = tune_with_cv(
                algorithm=algorithm,
                train_df=train,
                eval_splits=eval_splits,
                label_col=cfg["label_col"],
                pipeline_factory=lambda a, p: _build_pipeline(a, p, cfg),
                param_list=grid,
                num_folds=cv_folds,
                run_name=run_name,
                tags=_common_tags(cfg, phase="training",
                                  extra={"algorithm": algorithm}),
                extra_params=common_params,
            )
            record(run_id, run_name, algorithm, "training", best_params,
                   metrics, model)
            _print_metrics(metrics)
            print(f"  best params: {best_params}")

        training_winner_candidate = min(
            (c for c in candidates if c["phase"] == "training"),
            key=lambda c: c["metrics"]["val"]["rmse"],
        )
        winner = training_winner_candidate["algorithm"]
        winner_params = training_winner_candidate["params"]
        winner_run_id = training_winner_candidate["run_id"]
        winner_metrics = training_winner_candidate["metrics"]
        print(f"\n>>> Phase 1 winner (val_rmse): {winner} "
              f"(val_rmse={winner_metrics['val']['rmse']:.4f}, "
              f"common_test_rmse={winner_metrics['common_test']['rmse']:.4f})")
        print(f"    best params: {winner_params}")

        # ---- Phase 2: More-data — A vs (Phase 1 winner as B) -------------
        # A = SUBSET_A_YEARS (the two earliest training years); B = the full
        # TRAIN_VAL_YEARS pool, reused as the Phase-1 winner (which already
        # trained on it). Both are scored on the same common_test = TEST_YEAR,
        # so neither sees the test year — the comparison is fair and isolates
        # "does more training data help generalize to a future year?".
        print(f"\n=== Phase 2: More-data — A vs Phase-1 winner (= B) for {winner} ===")
        MlflowClient().set_tag(winner_run_id, "more_data_subset", "B")
        training_winner_candidate["extra_tags"]["more_data_subset"] = "B"
        print(f"  B = train-{winner} (run {winner_run_id}, "
              f"val_rmse={winner_metrics['val']['rmse']:.4f})")

        sub_train = train.filter(col(cfg["year_col"]).isin(SUBSET_A_YEARS))
        sub_count = sub_train.count()
        run_name = "more-data-A"
        print(f"\n--- {run_name}: years={SUBSET_A_YEARS} ({sub_count:,} rows) ---")
        if sub_count == 0:
            print("  Skipping A: empty.")
        else:
            run_id, metrics, model_a = train_and_log_run(
                splits={"train": sub_train, **eval_splits},
                label_col=cfg["label_col"],
                pipeline=_build_pipeline(winner, winner_params, cfg),
                run_name=run_name,
                tags=_common_tags(cfg, phase="more_data",
                                  extra={"algorithm": winner,
                                         "subset": "A",
                                         "more_data_subset": "A",
                                         "training_data": str(SUBSET_A_YEARS)}),
                extra_params={**common_params, **winner_params,
                              "subset": "A",
                              "subset_train_count": sub_count,
                              "n_train_years": len(SUBSET_A_YEARS),
                              "training_data": str(SUBSET_A_YEARS)},
            )
            record(run_id, run_name, winner, "more_data",
                   winner_params, metrics, model_a,
                   extra_tags={"subset": "A",
                               "more_data_subset": "A",
                               "training_data": str(SUBSET_A_YEARS)})
            _print_metrics(metrics)

        # ---- Summary -----------------------------------------------------
        # Sorted by val_rmse — the selection metric. common_test_rmse is
        # reported but never used to rank, so common_test stays an unbiased
        # held-out estimate. See [[project-ml-val-for-selection]] in memory.
        print("\n=== Summary (all candidates, sorted by val_rmse) ===")
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
            f"(reported only — common_test isn't used to rank candidates)"
        )

        # ---- Phase 3: Register every candidate ---------------------------
        # The within-run winner (lowest val_rmse) is the contender for
        # @champion. common_test is never used as a selection signal — it
        # stays held-out so the metric reported on it is unbiased.
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
            for k in ("more_data_subset", "subset"):
                if k in c["extra_tags"]:
                    tags[k] = c["extra_tags"][k]
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
            f"Phase 3: registering {len(candidates)} candidates to "
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
            f"\n=== Phase 3: registered {len(registered)} version(s) of "
            f"{cfg['experiment']} (ranked by val_rmse) ==="
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
            f"\nLoad champion: "
            f"mlflow.spark.load_model('models:/{cfg['experiment']}@champion')"
        )
        print(f"Tracking URI: {MLFLOW_TRACKING_URI}  Experiment: {cfg['experiment']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
