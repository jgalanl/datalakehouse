import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, expr, lit, month as spark_month,
    unix_timestamp, when, year as spark_year,
)
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampNTZType,
)

from common.spark_factory import get_spark
from common.iceberg_schema import schema_to_ddl
from common import nessie_manager as nessie

YEARS = [2023, 2024, 2025]
MONTHS = [1, 2, 3]
# Mismo identificador en ambas ramas (bronze vs silver); el ref activo decide a qué tabla apunta la lectura.
SOURCE = "nessie.nyc.yellow_taxi"
TARGET = "nessie.nyc.yellow_taxi"
TARGET_LOCATION = nessie.table_location("silver", "yellow_taxi")

VALID_LOCATION_MIN = 1
VALID_LOCATION_MAX = 265

# Diccionario de datos de la TLC (edición 2024).
VALID_VENDOR_IDS    = [1, 2, 6, 7]           # Creative Mobile Tech, Curb Mobility, Myle Technologies, Helix
VALID_RATE_CODES    = [1, 2, 3, 4, 5, 6, 99] # Estándar, JFK, Newark, Nassau/Westchester, Negociada, Grupo, Nulo/desconocido
VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]  # Flex Fare, Tarjeta, Efectivo, Sin cargo, Disputa, Desconocido, Anulado

# Eliminación de outliers por IQR: descarta las filas fuera de los límites Tukey "far out"
# [Q1-k*IQR, Q3+k*IQR] (k=3.0), calculados por RatecodeID.
IQR_K = 3.0
# Duración del viaje (segundos): solo sirve de base para su propio límite IQR; se descarta antes de escribir, no se guarda.
DURATION_COL = "_trip_duration_s"
OUTLIER_COLS = ["fare_amount", "total_amount", "trip_distance", DURATION_COL]

# Fracción máxima de filas que clean() puede descartar antes de que falle la auditoría.
MAX_DROP_RATIO = 0.3

# Esquema silver: NOT NULL en cada columna que clean() vuelve no nula de forma demostrable;
# nullable en los recargos que no filtra (extra, mta_tax, improvement_surcharge) y en los
# campos que dependen del año (congestion_surcharge, airport_fee, cbd_congestion_fee).
SILVER_SCHEMA = StructType([
    StructField("VendorID", LongType(), nullable=False),
    StructField("tpep_pickup_datetime", TimestampNTZType(), nullable=False),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), nullable=False),
    StructField("passenger_count", DoubleType(), nullable=False),
    StructField("trip_distance", DoubleType(), nullable=False),
    StructField("RatecodeID", DoubleType(), nullable=False),
    StructField("store_and_fwd_flag", StringType(), nullable=False),
    StructField("PULocationID", LongType(), nullable=False),
    StructField("DOLocationID", LongType(), nullable=False),
    StructField("payment_type", LongType(), nullable=False),
    StructField("fare_amount", DoubleType(), nullable=False),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=False),
    StructField("tolls_amount", DoubleType(), nullable=False),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=False),
    StructField("congestion_surcharge", DoubleType(), nullable=True),
    StructField("airport_fee", DoubleType(), nullable=True),
    StructField("cbd_congestion_fee", DoubleType(), nullable=True),
])


def remove_outliers(df: DataFrame) -> DataFrame:
    """Descarta las filas fuera de los límites Tukey "far out", calculados por RatecodeID.

    Los q1/q3 de cada código (un groupBy, <=7 filas recogidas) se incrustan en un
    CASE WHEN por columna, así el filtro corre en una sola pasada y sin join. Se recortan
    ambas colas.

    Los cuantiles son exactos (percentile, no percentile_approx): el sketch aproximado
    depende del particionado, así que la misma rodaja de bronze producía vallas —y por
    tanto filas— distintas en cada ejecución. Aquí es además más barato: percentile
    agrega sobre valores distintos (<= ~13 k por columna al mes), no sobre filas.
    """
    fences = df.groupBy("RatecodeID").agg(
        *[expr(f"percentile({c}, array(0.25, 0.75))").alias(c)
          for c in OUTLIER_COLS],
    ).collect()

    keep = lit(True)
    for c in OUTLIER_COLS:
        col_keep = lit(True)  # por defecto: sin límite -> la fila pasa
        for r in fences:
            q1, q3 = r[c]
            iqr = q3 - q1
            lo, hi = q1 - IQR_K * iqr, q3 + IQR_K * iqr
            col_keep = when(
                col("RatecodeID") == r["RatecodeID"],
                col(c).between(lo, hi),
            ).otherwise(col_keep)
        keep = keep & col_keep
    return df.filter(keep)


def clean(df: DataFrame) -> DataFrame:
    """Aplica las reglas de limpieza silver a una porción de bronze: primero el
    filtrado de validez (nulos, orden incorrecto, importes no positivos, códigos/IDs
    fuera de rango, duplicados) y después la eliminación de outliers por IQR."""
    valid = (
        df
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime").isNotNull())
        .filter(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
        .filter(col("trip_distance") >= 0)
        .filter(col("fare_amount") > 0)   
        .filter(col("total_amount") > 0)
        .filter(col("tip_amount") >= 0)
        .filter(col("tolls_amount") >= 0)
        .filter(col("PULocationID").between(VALID_LOCATION_MIN, VALID_LOCATION_MAX))
        .filter(col("DOLocationID").between(VALID_LOCATION_MIN, VALID_LOCATION_MAX))
        .filter(col("VendorID").isin(VALID_VENDOR_IDS))
        .filter(col("RatecodeID").isin(VALID_RATE_CODES))
        .filter(col("payment_type").isin(VALID_PAYMENT_TYPES))
        .filter(col("store_and_fwd_flag").isin("Y", "N"))
        .filter(col("passenger_count").between(1, 6))
        .dropDuplicates()
    )
    valid = valid.withColumn(
        DURATION_COL,
        unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"),
    )
    return remove_outliers(valid).drop(DURATION_COL)


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.table(TARGET)
        return True
    except Exception:
        return False


def validate(bronze_count: int, silver_count: int) -> tuple[bool, str]:
    """Paso de auditoría: devuelve (passed, reason). Nunca lanza excepción; el WAP decide qué hacer."""
    if silver_count == 0:
        return False, "silver vacío tras la limpieza"
    drop_ratio = (bronze_count - silver_count) / bronze_count
    if drop_ratio > MAX_DROP_RATIO:
        return False, (
            f"ratio de descarte {drop_ratio:.1%} supera el umbral {MAX_DROP_RATIO:.0%} "
            f"({bronze_count:,} -> {silver_count:,})"
        )
    return True, (
        f"{silver_count:,} conservadas, {bronze_count - silver_count:,} descartadas "
        f"({drop_ratio:.1%})"
    )


def wap_clean(
    spark: SparkSession, year_val: int, m: int, branch_name: str
) -> tuple[int, int, bool, str]:
    """Ciclo WAP: Write → Audit → Publish (merge) o rechazo. Nunca fusiona si falla;
    quien llama pone la rama en cuarentena. Devuelve (bronze, silver, passed, reason)."""
    label = f"{year_val}-{m:02d}"

    try:
        nessie.create_branch(spark, branch_name, from_ref="silver")

        nessie.set_spark_branch(spark, "bronze")
        # Cacheamos la porción de bronze: se lee tres veces (recuento, cuantiles, materialización de silver).
        bronze_df = spark.table(SOURCE).filter(
            (spark_year(col("tpep_pickup_datetime")) == year_val) &
            (spark_month(col("tpep_pickup_datetime")) == m)
        ).cache()
        try:
            bronze_count = bronze_df.count()
            print(f"  [{label}] Bronze: {bronze_count:,} filas")

            if bronze_count == 0:
                print(f"  [{label}] Sin datos bronze este mes, se omite")
                return 0, 0, True, "sin datos bronze"

            # Cacheamos la porción limpia: se materializa en el ref de bronze antes del
            # cambio, así la escritura nunca vuelve a resolver el origen contra la rama WAP.
            silver_df = clean(bronze_df).cache()
            try:
                silver_count = silver_df.count()
                print(f"  [{label}] Tras la limpieza: {silver_count:,} filas")

                nessie.set_spark_branch(spark, branch_name)

                if not table_exists(spark):
                    print(f"  [{label}] Creando tabla {TARGET}")
                    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {nessie.NAMESPACE}")
                    spark.sql(f"""
                        CREATE TABLE {TARGET} (
                            {schema_to_ddl(SILVER_SCHEMA)}
                        ) USING iceberg
                        PARTITIONED BY (months(tpep_pickup_datetime))
                        LOCATION '{TARGET_LOCATION}'
                        TBLPROPERTIES (
                            'format-version' = '2',
                            'write.spark.check-nullability' = 'false'
                        )
                    """)
                print(f"  [{label}] Añadiendo a {TARGET}")
                silver_df.writeTo(TARGET).append()
            finally:
                silver_df.unpersist()

            passed, reason = validate(bronze_count, silver_count)
            if passed:
                print(f"  [{label}] Auditoría superada: {reason}")
                nessie.merge_branch(spark, source=branch_name, into="silver")
            else:
                print(f"  [{label}] Auditoría FALLIDA: {reason}")
            return bronze_count, silver_count, passed, reason
        finally:
            bronze_df.unpersist()
    finally:
        nessie.set_spark_branch(spark, "silver")


def main():
    spark = get_spark(app_name="silver-clean-yellow-taxi")
    nessie.ensure_persistent_branches(spark)

    total_bronze = total_silver = processed = 0
    quarantined: list[tuple[str, str, str]] = []  # (etiqueta, review_tag, motivo)

    for year_val in YEARS:
        merged_in_year = 0
        for m in MONTHS:
            label = f"{year_val}-{m:02d}"
            branch_name = nessie.wap_branch_name("silver", "yellow_taxi", year_val, m)

            if nessie.ref_exists(spark, branch_name):
                print(f"  Eliminando rama WAP obsoleta '{branch_name}'")
                nessie.drop_branch(spark, branch_name)

            print(f"\n  [{label}] Limpieza WAP (rama: {branch_name})")
            bronze_count, silver_count, passed, reason = wap_clean(
                spark, year_val, m, branch_name
            )

            if not passed:
                # Cuarentena: congelamos el snapshot fallido como tag de revisión, borramos la rama y seguimos.
                review_tag = nessie.review_tag_name("silver", "yellow_taxi", year_val, m)
                print(f"  [{label}] Poniendo en cuarentena como tag '{review_tag}'")
                nessie.create_tag(spark, review_tag, from_ref=branch_name)
                nessie.drop_branch(spark, branch_name)
                quarantined.append((label, review_tag, reason))
                continue

            # Aprobado (o mes vacío): borramos la rama WAP.
            nessie.drop_branch(spark, branch_name)

            if bronze_count == 0:
                continue

            total_bronze += bronze_count
            total_silver += silver_count
            processed += 1
            merged_in_year += 1

        # Etiquetamos el año si se fusionó al menos un mes, aunque algún mes posterior
        # quedara en cuarentena: el tag sigue marcando un snapshot real de silver.
        if merged_in_year > 0:
            tag_name = nessie.yearly_tag_name("silver", "yellow_taxi", year_val)
            print(f"  Creando tag '{tag_name}' ({merged_in_year} mes(es) fusionado(s))")
            nessie.create_tag(spark, tag_name, from_ref="silver")

    if processed == 0 and not quarantined:
        print("No se procesó ningún mes. ¿Está bronze poblado?")
        sys.exit(1)

    cleaned = total_bronze - total_silver
    retention = total_silver / total_bronze if total_bronze > 0 else 0
    print(f"\nHecho. {processed} meses fusionados -> {TARGET}")
    print(f"  Bronze: {total_bronze:,}  Silver: {total_silver:,}  "
          f"Descartadas: {cleaned:,}  Retención: {retention:.1%}")

    if quarantined:
        print(f"\nMeses en cuarentena ({len(quarantined)}) — NO fusionados a silver:")
        for qlabel, qtag, qreason in quarantined:
            print(f"  [{qlabel}] {qtag}: {qreason}")

    print("\nReferencias Nessie tras el ETL silver:")
    nessie.print_references(spark)
    spark.stop()


if __name__ == "__main__":
    main()
