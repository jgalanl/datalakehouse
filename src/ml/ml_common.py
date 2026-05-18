"""Shared utilities for ML training scripts and diagnostic notebooks.

Read-only contract: every ML pipeline consumes the gold tables
(nessie.nyc.* on the main branch).
Each use case has its own training script with its own main(); this module
holds the building blocks they share — Spark/MLflow setup, year-stratified
splits, the StringIndexer (+ OneHot/Scaler) preprocessing pipeline, the
regression evaluator, the algorithm/grid catalogue and the single-run MLflow
logger.
"""

import os
import time
from datetime import datetime
from functools import reduce

# MLflow + MinIO env. spark-master inherits AWS_* from docker-compose but not
# MLFLOW_*; setting them before importing mlflow lets boto3 reach MinIO when
# artifacts are uploaded.
os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")

import mlflow  # noqa: E402
import mlflow.spark  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from mlflow.exceptions import MlflowException  # noqa: E402
from pyspark.ml import Pipeline, PipelineModel  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import (  # noqa: E402
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import (  # noqa: E402
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common.spark_factory import get_spark  # noqa: E402
from common import nessie_manager as nessie  # noqa: E402

MLFLOW_TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]


def progress(msg: str) -> None:
    """Print a timestamped progress line, flushed immediately.

    spark-submit buffers stdout, so long Spark stages (fit, count) otherwise
    look frozen. Explicit flush makes each step visible as it happens.
    """
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

# Year setup. TEST_YEARS is the out-of-distribution drift probe: 2025 holds
# out the post-COVID + congestion-fee era so we can measure how the model
# behaves under distribution shift. TRAIN_YEAR_SUBSETS are cumulative slices
# used to draw a learning-curve over historical depth (1y → 2y → 3y → 4y);
# all subsets share the same test sets so metrics are directly comparable.
TRAIN_YEAR_SUBSETS: list[list[int]] = [
    [2019],
    [2019, 2020],
    [2019, 2020, 2022],
    [2019, 2020, 2022, 2023],
]
TEST_YEARS = [2025]

# Partition count for the cached split DataFrames. The per-year unionByName in
# make_splits accumulates many uneven partitions, and the gold tables are large
# relative to a 1 GB executor heap — repartitioning to a fixed count keeps each
# cached columnar block small enough to build without an OOM.
SPLIT_PARTITIONS = 64


def get_ml_spark(app_name: str) -> SparkSession:
    """Build a Spark session pinned to main (gold ref)."""
    spark = get_spark(app_name=app_name)
    nessie.set_spark_branch(spark, "main")
    return spark


def setup_mlflow(experiment_name: str) -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(experiment_name)


def make_splits(
    df: DataFrame,
    year_col: str,
    oos_test_years: list[int],
    weights: tuple[float, float, float] = (0.7, 0.15, 0.15),
    seed: int = 42,
    n_partitions: int = SPLIT_PARTITIONS,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Stratified-by-year split into (train, val, test_id, test_ood).

    test_ood comes from oos_test_years (kept whole — the drift probe).
    The remaining "in-distribution" years are split per year using `weights`
    and unioned back, so every year contributes proportionally to train,
    val and test_id. Having both an in-distribution test (test_id) and an
    out-of-distribution test (test_ood) lets us disambiguate model quality
    from drift: a large test_id↔test_ood gap means drift, a small one means
    the model travels well across years.

    Each split is repartitioned to `n_partitions` before returning so callers
    can .cache() them on small executors without an OOM in the columnar cache
    builder — the per-year unionByName otherwise leaves a few oversized partitions.
    """
    in_dist = df.filter(~df[year_col].isin(oos_test_years))
    test_ood = df.filter(df[year_col].isin(oos_test_years))

    years = sorted(r[0] for r in in_dist.select(year_col).distinct().collect())
    parts: list[tuple[DataFrame, DataFrame, DataFrame]] = []
    for y in years:
        yr = in_dist.filter(in_dist[year_col] == y)
        t, v, ti = yr.randomSplit(list(weights), seed=seed)
        parts.append((t, v, ti))
    train = reduce(DataFrame.unionByName, (p[0] for p in parts))
    val = reduce(DataFrame.unionByName, (p[1] for p in parts))
    test_id = reduce(DataFrame.unionByName, (p[2] for p in parts))
    return (
        train.repartition(n_partitions),
        val.repartition(n_partitions),
        test_id.repartition(n_partitions),
        test_ood.repartition(n_partitions),
    )


def build_regression_pipeline(
    numerical_cols: list[str],
    categorical_cols: list[str],
    label_col: str,
    estimator,
    one_hot: bool = False,
    scale: bool = False,
) -> Pipeline:
    """StringIndex categoricals → optionally OneHot → VectorAssemble → optionally Scale → estimator.

    Tree-based estimators (GBT, RF) work fine with integer-encoded categoricals
    so leave one_hot=False, scale=False (the default). Linear models (LR) need
    one_hot=True + scale=True to be a fair baseline — otherwise StringIndexer
    output gets read as an ordinal variable and unscaled features make
    regularization meaningless.
    """
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in categorical_cols
    ]
    if one_hot:
        encoders = [
            OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe")
            for c in categorical_cols
        ]
        cat_feature_cols = [f"{c}_ohe" for c in categorical_cols]
    else:
        encoders = []
        cat_feature_cols = [f"{c}_idx" for c in categorical_cols]
    feature_cols = numerical_cols + cat_feature_cols
    # handleInvalid="error": silver guarantees the feature columns are non-NULL,
    # so a NULL here means a real bug (silver regressed, or gold left a hole).
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="raw_features" if scale else "features",
        handleInvalid="error",
    )
    stages: list = [*indexers, *encoders, assembler]
    if scale:
        stages.append(StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withMean=False,
            withStd=True,
        ))
    estimator.setLabelCol(label_col).setFeaturesCol("features")
    stages.append(estimator)
    return Pipeline(stages=stages)


# Algorithm catalogue. Each script's Phase 1 (algorithm comparison) iterates
# over these three estimators with the DEFAULT_PARAMS shown below; Phase 2
# (tuning) iterates over PARAM_GRIDS[winner].
def make_estimator(algorithm: str, **params):
    if algorithm == "lr":
        return LinearRegression(**params)
    if algorithm == "rf":
        return RandomForestRegressor(**params)
    if algorithm == "gbt":
        return GBTRegressor(**params)
    raise ValueError(f"Unknown algorithm: {algorithm!r}")


def algorithm_pipeline_config(algorithm: str) -> dict:
    """Preprocessing config per algorithm — linear models need OHE + scaling."""
    if algorithm == "lr":
        return {"one_hot": True, "scale": True}
    return {"one_hot": False, "scale": False}


DEFAULT_PARAMS: dict[str, dict] = {
    "lr": {"regParam": 0.01, "elasticNetParam": 0.0},
    "rf": {"numTrees": 20, "maxDepth": 8, "seed": 42},
    "gbt": {"maxIter": 50, "maxDepth": 6, "stepSize": 0.1, "seed": 42},
}

# Compact grids — total runtime stays manageable on a local Spark cluster.
PARAM_GRIDS: dict[str, list[dict]] = {
    "lr": [
        {"regParam": 0.0, "elasticNetParam": 0.0},
        {"regParam": 0.01, "elasticNetParam": 0.0},
        {"regParam": 0.1, "elasticNetParam": 0.0},
        {"regParam": 0.01, "elasticNetParam": 0.5},
    ],
    "rf": [
        {"numTrees": 20, "maxDepth": 6, "seed": 42},
        {"numTrees": 20, "maxDepth": 10, "seed": 42},
        {"numTrees": 50, "maxDepth": 6, "seed": 42},
        {"numTrees": 50, "maxDepth": 10, "seed": 42},
    ],
    "gbt": [
        {"maxIter": 50, "maxDepth": 4, "stepSize": 0.1, "seed": 42},
        {"maxIter": 50, "maxDepth": 6, "stepSize": 0.1, "seed": 42},
        {"maxIter": 50, "maxDepth": 8, "stepSize": 0.1, "seed": 42},
        {"maxIter": 100, "maxDepth": 6, "stepSize": 0.05, "seed": 42},
        {"maxIter": 100, "maxDepth": 6, "stepSize": 0.1, "seed": 42},
    ],
}


def evaluate_regression(
    model: PipelineModel, df: DataFrame, label_col: str,
) -> dict[str, float]:
    """Return {rmse, mae, r2} on df.

    model.transform is lazy, so each evaluator would otherwise re-run the
    full preprocessing + prediction. Project to just (label, prediction) and
    cache once — the three metrics then share a single transform pass.
    """
    predictions = model.transform(df).select(label_col, "prediction").cache()
    try:
        metrics: dict[str, float] = {}
        for metric_name in ("rmse", "mae", "r2"):
            evaluator = RegressionEvaluator(
                labelCol=label_col, predictionCol="prediction", metricName=metric_name,
            )
            metrics[metric_name] = float(evaluator.evaluate(predictions))
        return metrics
    finally:
        predictions.unpersist()


def log_split_metrics(metrics: dict[str, float], split: str) -> None:
    """Log metrics to MLflow with a split prefix (train_rmse, test_rmse, ...)."""
    for name, value in metrics.items():
        mlflow.log_metric(f"{split}_{name}", value)


def train_and_log_run(
    *,
    splits: dict[str, DataFrame],
    label_col: str,
    pipeline: Pipeline,
    run_name: str,
    tags: dict,
    extra_params: dict | None = None,
) -> tuple[str, dict[str, dict[str, float]]]:
    """Fit `pipeline` on splits["train"], evaluate on every split, log to MLflow.

    Returns (run_id, {split_name: {metric_name: value}}). Caller decides the
    workflow (which algorithm, which grid, which year-subset) — this helper
    only knows how to log one fitted model. The run_id lets the caller hand
    the run to the Model Registry afterwards via `register_final_model`.
    """
    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        mlflow.set_tags(tags)
        if extra_params:
            mlflow.log_params(extra_params)

        progress(f"  [{run_name}] fitting pipeline ({len(pipeline.getStages())} stages)...")
        t0 = time.perf_counter()
        model = pipeline.fit(splits["train"])
        progress(f"  [{run_name}] fit done in {time.perf_counter() - t0:.1f}s")

        all_metrics: dict[str, dict[str, float]] = {}
        for split_name, split_df in splits.items():
            t1 = time.perf_counter()
            metrics = evaluate_regression(model, split_df, label_col)
            log_split_metrics(metrics, split_name)
            all_metrics[split_name] = metrics
            progress(f"  [{run_name}] evaluated {split_name} in {time.perf_counter() - t1:.1f}s")

        progress(f"  [{run_name}] logging model to MLflow...")
        t2 = time.perf_counter()
        mlflow.spark.log_model(model, artifact_path="model")
        progress(f"  [{run_name}] model logged in {time.perf_counter() - t2:.1f}s")
    return run_id, all_metrics


def register_final_model(
    *,
    run_id: str,
    name: str,
    version_description: str,
    version_tags: dict[str, str],
    registered_model_description: str,
    registered_model_tags: dict[str, str],
    promotion_metric: str,
    promotion_guard_metric: str,
    promotion_guard_tolerance: float = 0.02,
):
    """Register `run_id`'s model as a new version of `name` and apply a
    Pareto-style champion / challenger pattern. Assumes lower-is-better
    metrics (rmse / mae) — the only kind the project's regression use cases
    actually use.

    On first registration of a new model name, the registered model is
    created with `registered_model_*` metadata so the model card has stable
    info (source, target, features, framework). Subsequent runs do NOT
    overwrite the model-level description / tags — manual UI edits survive.
    Every run sets `version_*` on its own version (algorithm, scores, train_years).

    Promotion logic:
    - If no @champion exists yet → new version becomes @champion.
    - Else, promote only when BOTH criteria hold:
        (a) primary — `promotion_metric` strictly improves (new < champion).
        (b) guard — `promotion_guard_metric` does not degrade beyond
            `promotion_guard_tolerance` relative to the champion
            (new <= champion * (1 + tolerance)). Pareto-style: the new
            model can't trade generalization for a primary win.
      Otherwise the new version stays as @challenger.
    On promotion, the previous champion is demoted to @challenger.

    The model artifact itself lives in MinIO (bucket `mlflow-artifacts`).
    The registry only stores a versioned pointer + metadata in the tracking
    backend (Postgres). Consumers load via `models:/{name}@champion`.

    Returns (mv, alias_assigned, promoted_to_champion).
    """
    client = MlflowClient()

    # 1. First-creation hook for the registered model.
    try:
        client.get_registered_model(name)
    except MlflowException:
        client.create_registered_model(
            name=name,
            description=registered_model_description or "",
            tags=registered_model_tags or {},
        )

    # 2. Register a new version. mlflow.register_model waits until the
    #    version is READY (default await_registration_for=300s).
    mv = mlflow.register_model(
        model_uri=f"runs:/{run_id}/model",
        name=name,
    )

    # 3. Per-version metadata.
    client.update_model_version(
        name=name, version=mv.version, description=version_description,
    )
    for k, v in version_tags.items():
        client.set_model_version_tag(
            name=name, version=mv.version, key=k, value=str(v),
        )

    # 4. Champion / challenger.
    new_metrics = client.get_run(run_id).data.metrics

    try:
        cur_champ = client.get_model_version_by_alias(name, "champion")
    except MlflowException:
        # No champion yet — first version is the champion by default.
        client.set_registered_model_alias(name, "champion", mv.version)
        print(f"  v{mv.version} set as @champion (first version of {name})")
        return mv, "champion", True

    champ_metrics = client.get_run(cur_champ.run_id).data.metrics
    new_primary = new_metrics[promotion_metric]
    champ_primary = champ_metrics[promotion_metric]
    new_guard = new_metrics[promotion_guard_metric]
    champ_guard = champ_metrics[promotion_guard_metric]

    # Primary criterion: strict improvement (lower is better).
    primary_better = new_primary < champ_primary
    primary_op = "<" if primary_better else ">="
    primary_phrase = (
        f"{promotion_metric} {new_primary:.4f} {primary_op} "
        f"v{cur_champ.version}'s {champ_primary:.4f}"
    )

    # Guard criterion: must not degrade beyond tolerance (lower is better).
    threshold = champ_guard * (1 + promotion_guard_tolerance)
    guard_ok = new_guard <= threshold
    guard_op = "<=" if guard_ok else ">"
    guard_phrase = (
        f"{promotion_guard_metric} {new_guard:.4f} {guard_op} "
        f"{threshold:.4f} ({promotion_guard_tolerance*100:.0f}% tol of "
        f"v{cur_champ.version}'s {champ_guard:.4f})"
    )

    if primary_better and guard_ok:
        client.set_registered_model_alias(name, "champion", mv.version)
        client.set_registered_model_alias(name, "challenger", cur_champ.version)
        print(f"  v{mv.version} promoted to @champion")
        print(f"    primary: {primary_phrase}")
        print(f"    guard:   {guard_phrase}")
        print(f"    v{cur_champ.version} demoted to @challenger")
        return mv, "champion", True
    else:
        client.set_registered_model_alias(name, "challenger", mv.version)
        print(f"  v{mv.version} stays as @challenger")
        if not primary_better:
            print(f"    primary failed: {primary_phrase}")
        else:
            print(f"    primary OK:    {primary_phrase}")
            print(f"    guard failed:  {guard_phrase}")
        return mv, "challenger", False
