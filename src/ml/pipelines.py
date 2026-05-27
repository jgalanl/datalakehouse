"""Spark ML preprocessing pipeline factory (StringIndex → scale → assemble)."""

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
    VectorIndexer,
)


# VectorIndexer treats an assembled-vector column as categorical when it has
# at most this many distinct values — low-cardinality fields (borough, vendor,
# ratecode, payment type) become genuine categorical splits while the ~265-
# value location IDs fall through to continuous.
TREE_MAX_CATEGORIES = 32

# maxBins for the tree estimators. The StringIndexer-coded location IDs carry
# ~265 levels (+1 for handleInvalid="keep"); Spark's buildMetadata requires
# maxBins >= the arity of every feature still treated as categorical. The
# default (32) fails before VectorIndexer's continuous re-stamp can take
# effect. 300 clears headroom and is harmless for continuous features.
TREE_MAX_BINS = 300


def build_regression_pipeline(
    numerical_cols: list[str],
    categorical_cols: list[str],
    label_col: str,
    estimator,
    one_hot: bool = False,
    scale: str | None = None,        # "standard" or None
    vector_index: bool = False,
) -> Pipeline:
    """StringIndex categoricals → optional OneHot → numerical scaling →
    assemble → optional VectorIndexer → estimator.

    scale="standard": StandardScaler — GLR (so regularization is meaningful).
    vector_index=True: re-stamp the assembled vector so trees treat
                      low-cardinality columns as categorical splits and the
                      ~265-value location IDs as continuous (GBT, RFR).
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
    stages: list = [*indexers, *encoders]

    # handleInvalid="error": silver guarantees the feature columns are non-NULL,
    # so a NULL at this stage means a real bug (silver regressed or gold left
    # a hole).
    if scale == "standard":
        # Build the numerical block on its own so we can scale it, then re-
        # assemble with the (unscaled) one-hot block. Scaling the one-hot
        # columns instead would divide each by its near-zero std and blow
        # rare categories up to huge magnitudes.
        stages.append(VectorAssembler(
            inputCols=numerical_cols,
            outputCol="numerical_raw",
            handleInvalid="error",
        ))
        stages.append(StandardScaler(
            inputCol="numerical_raw", outputCol="numerical_scaled",
            withMean=False, withStd=True,
        ))
        assembler_inputs = ["numerical_scaled"] + cat_feature_cols
    else:
        assembler_inputs = numerical_cols + cat_feature_cols

    stages.append(VectorAssembler(
        inputCols=assembler_inputs,
        outputCol="assembled_features",
        handleInvalid="error",
    ))
    features_col = "assembled_features"

    if vector_index:
        # handleInvalid="keep": a held-out split may carry a categorical value
        # unseen at fit time — bucket it rather than failing the transform.
        stages.append(VectorIndexer(
            inputCol=features_col, outputCol="indexed_features",
            maxCategories=TREE_MAX_CATEGORIES, handleInvalid="keep",
        ))
        features_col = "indexed_features"

    estimator.setLabelCol(label_col).setFeaturesCol(features_col)
    stages.append(estimator)
    return Pipeline(stages=stages)
