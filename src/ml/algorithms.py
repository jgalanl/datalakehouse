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

TREE_ALGORITHMS: frozenset[str] = frozenset({"gbt", "rfr"})


def make_estimator(algorithm: str, **params):
    return ALGORITHM_SPECS[algorithm]["estimator_cls"](**params)


def algorithm_pipeline_config(algorithm: str) -> dict:
    """Configuración de preprocesado por algoritmo."""
    return dict(ALGORITHM_SPECS[algorithm]["pipeline_config"])


# Familia/enlace de GLR, elegidos por caso de uso al construir el estimador.
GLR_FAMILY_PRESETS: dict[str, dict] = {
    "gaussian": {"family": "gaussian", "link": "identity"},
    "gamma":    {"family": "gamma",    "link": "log"},
    "poisson":  {"family": "poisson",  "link": "log"},
}


# GBT
_GBT_BASE: dict = {
    "maxIter": 50, "maxDepth": 6, "stepSize": 0.1,
    "subsamplingRate": 1.0, "featureSubsetStrategy": "all",
    "maxBins": TREE_MAX_BINS, "seed": 42,
}
# RFR
_RFR_BASE: dict = {
    "numTrees": 100, "maxDepth": 6,
    "subsamplingRate": 1.0, "featureSubsetStrategy": "auto",
    "maxBins": TREE_MAX_BINS, "seed": 42,
}

# Cuatro puntos por algoritmo: mismo presupuesto, acotado para que el CV salga barato
# (4 × 3 folds = 12 fits cada uno). GLR barre regParam, GBT barre la profundidad más un
# tanteo de GBM estocástico, y RFR barre el tamaño del bosque más un tanteo de profundidad.
FULL_PARAM_GRIDS: dict[str, list[dict]] = {
    # Saltamos regParam 0 y 0.01: con gamma/log la matriz es deficiente de rango ahí y
    # rompe Cholesky/LBFGS. El óptimo cae en la banda 0.1–3.0 de todas formas.
    "glr": [
        {"regParam": 0.1, "maxIter": 25},
        {"regParam": 0.5, "maxIter": 25},
        {"regParam": 1.0, "maxIter": 25},
        {"regParam": 3.0, "maxIter": 25},
    ],
    "gbt": [
        {**_GBT_BASE, "maxDepth": 4},
        {**_GBT_BASE, "maxDepth": 5},
        dict(_GBT_BASE),
        {**_GBT_BASE, "maxIter": 60,
         "subsamplingRate": 0.7, "featureSubsetStrategy": "sqrt"},
    ],
    "rfr": [
        {**_RFR_BASE, "numTrees": 50},
        dict(_RFR_BASE),
        {**_RFR_BASE, "numTrees": 200},
        {**_RFR_BASE, "maxDepth": 8},
    ],
}

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
    args = sys.argv[1:] if argv is None else argv
    return "--fast" in args or os.environ.get("ML_FAST_MODE") == "1"
