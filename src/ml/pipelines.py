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
    # Un único StringIndexer/OneHotEncoder multi-columna: ajustan todas las categóricas sobre los datos.
    idx_cols = [f"{c}_idx" for c in categorical_cols]
    stages: list = []
    cat_feature_cols = idx_cols
    if categorical_cols:
        stages.append(StringIndexer(
            inputCols=categorical_cols, outputCols=idx_cols, handleInvalid="keep",
        ))
        if one_hot:
            cat_feature_cols = [f"{c}_ohe" for c in categorical_cols]
            stages.append(OneHotEncoder(
                inputCols=idx_cols, outputCols=cat_feature_cols,
            ))

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
