from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
    VectorIndexer,
)


TREE_MAX_CATEGORIES = 32
TREE_MAX_BINS = 300


def build_regression_pipeline(
    numerical_cols: list[str],
    categorical_cols: list[str],
    label_col: str,
    estimator,
    one_hot: bool = False,
    scale: str | None = None,
    vector_index: bool = False,
) -> Pipeline:
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

    if scale == "standard":
        # Solo se escalan las numéricas: las one-hot tienen desviación casi nula, así
        # que escalarlas dispararía las categorías raras a magnitudes enormes.
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
        # keep = agrupa las categorías no vistas en el fit en vez de lanzar error.
        stages.append(VectorIndexer(
            inputCol=features_col, outputCol="indexed_features",
            maxCategories=TREE_MAX_CATEGORIES, handleInvalid="keep",
        ))
        features_col = "indexed_features"

    estimator.setLabelCol(label_col).setFeaturesCol(features_col)
    stages.append(estimator)
    return Pipeline(stages=stages)
