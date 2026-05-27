"""Data preparation for ML training: last-year holdout split + cyclical encoding."""

import math
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, cos as spark_cos, lit, sin as spark_sin
from pyspark.sql.types import DoubleType


# Last-year holdout: TEST_YEAR is *entirely* held out — common_test is every
# row of that year and contains zero rows the model has ever seen during
# training or hyperparameter selection. The metric on common_test is therefore
# an unbiased generalization estimate to a future year, not contaminated by
# the within-year temporal autocorrelation a random per-year split leaks. The
# trade-off is that the per-year breakdown of common_test collapses to a
# single year (TEST_YEAR), so the diagnostic notebook tells the drift story
# by comparing TEST_YEAR's metric to the per-TRAIN_VAL_YEAR breakdown instead.
TEST_YEAR: int = 2025
TRAIN_VAL_YEARS: list[int] = [2019, 2020, 2022, 2023]

# Phase 2 (more-data) compares two training-pool sizes, BOTH evaluated on
# common_test = TEST_YEAR — so neither A nor B ever sees the test year and
# the comparison is fair. A = the two earliest years (half the pool); B = all
# of TRAIN_VAL_YEARS, reused as the Phase-1 winner (no refit needed).
SUBSET_A_YEARS: list[int] = [2019, 2020]

# Partition count for the cached split DataFrames. The per-year unionByName in
# make_splits accumulates many uneven partitions; repartitioning keeps each
# cached columnar block small enough to build on a 2 GB executor without OOM.
SPLIT_PARTITIONS = 64


def make_splits(
    df: DataFrame,
    year_col: str,
    train_val_years: list[int] | None = None,
    test_year: int | None = None,
    val_frac: float = 0.15,
    seed: int = 42,
    n_partitions: int = SPLIT_PARTITIONS,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Last-year holdout split into (train, val, common_test).

    `common_test` is every row where `year_col == test_year` — the model
    has never seen any of it during training or selection. Within
    `train_val_years`, the rows are stratified-by-year random-split into
    train (~ 1 - val_frac per year) and val (~ val_frac per year). Val is
    a model-selection signal; some residual within-year autocorrelation
    between train and val is acceptable. The honest generalization
    estimate is the metric on common_test.

    The defaults pull from the module constants TRAIN_VAL_YEARS / TEST_YEAR.
    """
    train_val_years = train_val_years or TRAIN_VAL_YEARS
    test_year = test_year if test_year is not None else TEST_YEAR

    common_test = df.filter(col(year_col) == test_year)

    train_weight = 1.0 - val_frac
    parts: list[tuple[DataFrame, DataFrame]] = []
    for y in train_val_years:
        yr = df.filter(col(year_col) == y)
        t, v = yr.randomSplit([train_weight, val_frac], seed=seed)
        parts.append((t, v))
    train = reduce(DataFrame.unionByName, (p[0] for p in parts))
    val = reduce(DataFrame.unionByName, (p[1] for p in parts))
    return (
        train.repartition(n_partitions),
        val.repartition(n_partitions),
        common_test.repartition(n_partitions),
    )


# Hour-of-day (0..23) and day-of-week (0..6) wrap around. Integer encoding
# puts 23h numerically far from 0h even though they're adjacent. sin/cos at
# the right period fixes this for GLR; trees split on integers fine and
# don't benefit.
CYCLICAL_COLS: dict[str, int] = {
    "pickup_hour": 24,
    "pickup_dow": 7,
    "hour_of_day": 24,
    "hour_dow": 7,
}


def add_cyclical_features(
    df: DataFrame, numerical_cols: list[str],
) -> tuple[DataFrame, list[str]]:
    """Append sin/cos columns for each cyclical numerical feature in the input.

    Returns (df_with_extra_cols, added_col_names). The original integer cols
    are kept in the DataFrame — tree-model pipelines still consume them.
    """
    added: list[str] = []
    out = df
    for c in numerical_cols:
        if c not in CYCLICAL_COLS:
            continue
        period = CYCLICAL_COLS[c]
        angle = col(c).cast(DoubleType()) * lit(2.0 * math.pi / period)
        out = (
            out.withColumn(f"{c}_sin", spark_sin(angle))
               .withColumn(f"{c}_cos", spark_cos(angle))
        )
        added.extend([f"{c}_sin", f"{c}_cos"])
    return out, added


def resolve_numerical_cols(
    algorithm: str, base_numerical_cols: list[str],
) -> list[str]:
    """Trees (GBT, RFR) keep the integer hour/dow. GLR swaps them for sin/cos."""
    # Late import to avoid the splits ↔ algorithms cycle (algorithms imports
    # from pipelines; splits is consumed by both).
    from ml.algorithms import TREE_ALGORITHMS

    if algorithm in TREE_ALGORITHMS:
        return base_numerical_cols
    out: list[str] = []
    for c in base_numerical_cols:
        if c in CYCLICAL_COLS:
            out.extend([f"{c}_sin", f"{c}_cos"])
        else:
            out.append(c)
    return out
