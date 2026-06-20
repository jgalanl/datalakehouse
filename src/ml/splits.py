import math
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, cos as spark_cos, lit, sin as spark_sin
from pyspark.sql.types import DoubleType



TEST_YEAR: int = 2025
TRAIN_VAL_YEARS: list[int] = [2023, 2024]


SUBSET_A_YEARS: list[int] = [2023]

# Reparticionamos para que cada bloque columnar cacheado quepa en un executor de 3 GB.
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
    """Holdout del último año: (train, val, common_test).

    common_test son todas las filas de test_year, que nunca se tocan durante el
    entrenamiento ni la selección. train/val es un split aleatorio por año de train_val_years.
    """
    train_val_years = train_val_years or TRAIN_VAL_YEARS
    test_year = test_year if test_year is not None else TEST_YEAR

    # Cacheamos una vez para que randomSplit sea determinista. Si no, el padre lazy
    # se recalcula en cada acción y las filas se mueven entre (o fuera de) los splits.
    df = df.cache()

    common_test = df.filter(col(year_col) == test_year)

    train_weight = 1.0 - val_frac
    parts: list[tuple[DataFrame, DataFrame]] = []
    for y in train_val_years:
        yr = df.filter(col(year_col) == y)
        t, v = yr.randomSplit([train_weight, val_frac], seed=seed)
        parts.append((t, v))
    train = reduce(DataFrame.unionByName, (p[0] for p in parts)).repartition(n_partitions)
    val = reduce(DataFrame.unionByName, (p[1] for p in parts)).repartition(n_partitions)
    common_test = common_test.repartition(n_partitions)

    # Materializamos los hijos en su propia caché (con el padre aún cacheado, así
    # randomSplit es determinista) y soltamos el padre: nadie más lo consume.
    for child in (train, val, common_test):
        child.cache().count()
    df.unpersist()
    return train, val, common_test


# Codificamos hora (0..23) y día de la semana (0..6) como sin/cos para que las 23h queden
# pegadas a las 0h. GLR lo necesita; los árboles dividen sobre los enteros, así que no.
CYCLICAL_COLS: dict[str, int] = {
    "pickup_hour": 24,
    "pickup_dow": 7,
    "hour_of_day": 24,
    "hour_dow": 7,
}


def add_cyclical_features(
    df: DataFrame, numerical_cols: list[str],
) -> tuple[DataFrame, list[str]]:
    """Añade sin/cos por cada feature cíclica, manteniendo también las columnas enteras
    (los árboles las siguen usando). Devuelve (df, nombres de las columnas añadidas)."""
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
    """Los árboles (GBT, RFR) mantienen la hora/día enteros; GLR cambia al par sin/cos."""
    from ml.algorithms import TREE_ALGORITHMS  # import tardío para romper un ciclo

    if algorithm in TREE_ALGORITHMS:
        return base_numerical_cols
    out: list[str] = []
    for c in base_numerical_cols:
        if c in CYCLICAL_COLS:
            out.extend([f"{c}_sin", f"{c}_cos"])
        else:
            out.append(c)
    return out
