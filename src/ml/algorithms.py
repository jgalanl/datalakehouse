"""Algorithm catalogue + hyperparameter grids + fast-mode constants.

Three regression estimators: GLM (GLR with use-case-fit family/link), boosting
tree ensemble (GBT), bagging tree ensemble (RFR). Single source of truth for
the estimator class + the preprocessing pipeline knobs per algorithm.

GLR family/link are supplied per use case via GLR_FAMILY_PRESETS — gaussian/
identity for vanilla regression, gamma/log for right-skewed positive targets
(fare, duration), poisson/log for counts (n_pickups). GBT and RFR share the
same tree preprocessing (vector_index), so the catalogue gives the thesis a
linear/parametric model + two complementary tree ensembles (variance-reducing
bagging vs bias-reducing boosting).
"""

import os
import sys

from pyspark.ml.regression import (
    GBTRegressor,
    GeneralizedLinearRegression,
    RandomForestRegressor,
)

from ml.pipelines import TREE_MAX_BINS


CV_NUM_FOLDS = 3
FAST_CV_NUM_FOLDS = 2
FAST_SAMPLE_FRACTION = 0.01
# Default sample of the gold table used by every (non-fast) train.py run.
# Halves CV/fit time while leaving each fold with ~millions of rows — plenty
# for stable RMSE on the three regression use cases.
SAMPLE_FRACTION = 1.00


ALGORITHM_SPECS: dict[str, dict] = {
    "glr": {
        "estimator_cls": GeneralizedLinearRegression,
        "pipeline_config": {"one_hot": True, "scale": "standard"},
    },
    "gbt": {
        "estimator_cls": GBTRegressor,
        "pipeline_config": {"vector_index": True},
    },
    "rfr": {
        "estimator_cls": RandomForestRegressor,
        "pipeline_config": {"vector_index": True},
    },
}
ALGORITHMS: tuple[str, ...] = tuple(ALGORITHM_SPECS)

# Tree-based algorithms split on integer values directly, so they keep the
# raw hour/dow columns; non-tree algorithms get the cyclical sin/cos pair.
# Used by `ml.splits.resolve_numerical_cols` to pick the right encoding.
TREE_ALGORITHMS: frozenset[str] = frozenset({"gbt", "rfr"})


def make_estimator(algorithm: str, **params):
    return ALGORITHM_SPECS[algorithm]["estimator_cls"](**params)


def algorithm_pipeline_config(algorithm: str) -> dict:
    """Preprocessing config per algorithm."""
    return dict(ALGORITHM_SPECS[algorithm]["pipeline_config"])


# GLR family/link presets selected per use case at call time.
GLR_FAMILY_PRESETS: dict[str, dict] = {
    "gaussian": {"family": "gaussian", "link": "identity"},
    "gamma":    {"family": "gamma",    "link": "log"},
    "poisson":  {"family": "poisson",  "link": "log"},
}


# Bases exclude GLR family/link — those are merged in from the use case
# config so a single grid serves all three use cases. For GBT, the
# subsamplingRate / featureSubsetStrategy entries are set to Spark defaults
# (1.0 / "all"); the full grid varies subsamplingRate per Friedman (2002)
# "Stochastic Gradient Boosting" plus a single per-split column subsampling
# probe ("sqrt"). minInstancesPerNode is intentionally not tuned — it overlaps
# heavily with maxDepth, which we already sweep over 4/5/6.
_GBT_BASE: dict = {
    "maxIter": 50, "maxDepth": 6, "stepSize": 0.1,
    "subsamplingRate": 1.0, "featureSubsetStrategy": "all",
    "maxBins": TREE_MAX_BINS, "seed": 42,
}
# Spark's RandomForestRegressor uses `featureSubsetStrategy="auto"`, which
# resolves to `onethird` for regression (Breiman's recommended p/3 for RF
# regression). Left explicit here so the base is self-documenting.
_RFR_BASE: dict = {
    "numTrees": 100, "maxDepth": 6,
    "subsamplingRate": 1.0, "featureSubsetStrategy": "auto",
    "maxBins": TREE_MAX_BINS, "seed": 42,
}

# Per-algorithm grids consumed by the single Training phase. Sized to 4
# points each so every algorithm gets the same number of tuning trials
# (equal opportunity across GLR/GBT/RFR), and capped at 4 to keep CV cost
# bounded on full-data (un-sampled) runs — 4 points × CV_NUM_FOLDS=3 = 12
# fits/algorithm. Each row varies one (or several) knob versus the base —
# keeps the design intent visible: GLR sweeps regParam log-spaced bracketing
# the likely optimum; GBT explores depth (Friedman 2001) plus the two
# stochastic-GBM regularizers `subsamplingRate` (row subsampling) +
# `featureSubsetStrategy` (per-split column subsampling) per Friedman 2002;
# RFR explores forest size and tree depth (Breiman 2001).
# `minInstancesPerNode` is intentionally not tuned for either tree algorithm
# — it overlaps with maxDepth, which we already sweep.
FULL_PARAM_GRIDS: dict[str, list[dict]] = {
    # GLR with gamma/log on this design matrix is rank-deficient (OHE'd
    # high-cardinality location_id × ratecode × payment_type), so regParam=0
    # and regParam=0.01 trigger Cholesky → singular covariance → LBFGS
    # giving up. The 4-point grid spans 0.1 → 3.0 log-spaced with the infill
    # at 0.5 retained near the likely optimum (regParam=1.0 has won before);
    # the far end (10.0) was dropped — the optimum sits in the lower band.
    "glr": [
        {"regParam": 0.1, "maxIter": 25},
        {"regParam": 0.5, "maxIter": 25},
        {"regParam": 1.0, "maxIter": 25},
        {"regParam": 3.0, "maxIter": 25},
    ],
    "gbt": [
        # Depth sweep at default shrinkage. Capped at depth 6 (was 8):
        # boosting is sequential, so depth-8 × 100-iter fits dominated the
        # CV wall-clock while barely moving RMSE on this ~9-feature design.
        {**_GBT_BASE, "maxDepth": 4},
        {**_GBT_BASE, "maxDepth": 5},
        dict(_GBT_BASE),
        # Combined stochastic regularizers (Friedman 2002): row subsampling
        # + per-split column subsampling. One combined probe instead of two
        # separate ones — after one-hot we have ~10 effective groups, so
        # the two stochastic axes interact rather than separate cleanly. The
        # 0.7 row subsample also makes each tree cheaper, partly offsetting
        # the maxIter=60 budget. The pure shrinkage rows (maxIter=60 ×
        # stepSize=0.05) were dropped in the trim — depth is the stronger
        # dial on this small feature design.
        {**_GBT_BASE, "maxIter": 60,
         "subsamplingRate": 0.7, "featureSubsetStrategy": "sqrt"},
    ],
    "rfr": [
        # Forest size sweep — RF's main dial. More trees ⇒ lower variance
        # with diminishing returns.
        {**_RFR_BASE, "numTrees": 50},
        dict(_RFR_BASE),
        {**_RFR_BASE, "numTrees": 200},
        # Depth probe — upper bound only (depth=4 underfits a 9-feature
        # design; depth=8 is the meaningful contrast vs base depth=6). The
        # forest-size×depth combo and the "sqrt" column-subset probe were
        # dropped in the trim — forest size and depth are the primary dials.
        {**_RFR_BASE, "maxDepth": 8},
    ],
}

# Fast-mode grids — tiny so the full 4-phase + baseline workflow runs in
# minutes against a 1 %-sampled gold table. Verifies the pipeline completes
# (including the stochastic-GBM code path); metrics are not meaningful.
_FAST_GBT_BASE: dict = {
    "maxIter": 5, "maxDepth": 3, "stepSize": 0.1,
    "subsamplingRate": 1.0, "featureSubsetStrategy": "all",
    "maxBins": TREE_MAX_BINS, "seed": 42,
}
_FAST_RFR_BASE: dict = {
    "numTrees": 10, "maxDepth": 3,
    "subsamplingRate": 1.0, "featureSubsetStrategy": "auto",
    "maxBins": TREE_MAX_BINS, "seed": 42,
}
FAST_FULL_PARAM_GRIDS: dict[str, list[dict]] = {
    "glr": [{"regParam": 0.1, "maxIter": 10}, {"regParam": 1.0, "maxIter": 10}],
    "gbt": [
        dict(_FAST_GBT_BASE),
        {**_FAST_GBT_BASE, "maxIter": 8, "maxDepth": 4,
         "subsamplingRate": 0.7, "featureSubsetStrategy": "sqrt"},
    ],
    "rfr": [
        dict(_FAST_RFR_BASE),
        {**_FAST_RFR_BASE, "numTrees": 20, "maxDepth": 4},
    ],
}


def fast_mode_enabled(argv: list[str] | None = None) -> bool:
    """--fast on the spark-submit command line or ML_FAST_MODE=1 in env."""
    args = sys.argv[1:] if argv is None else argv
    return "--fast" in args or os.environ.get("ML_FAST_MODE") == "1"
