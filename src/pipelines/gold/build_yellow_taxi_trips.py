import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast, col, count, hour, month as spark_month,
    sum as spark_sum, unix_timestamp, weekday, weekofyear, when,
    year as spark_year,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampNTZType,
)

from common.spark_factory import get_spark
from common.iceberg_schema import schema_to_ddl
from common import nessie_manager as nessie

YEARS = [2023, 2024, 2025]
MONTHS = [1, 2, 3]
# SOURCE_* se leen en silver, TARGET se escribe en main; la rama activa selecciona la capa.
SOURCE_TRIPS = "nessie.nyc.yellow_taxi"
SOURCE_ZONES = "nessie.nyc.taxi_zone_lookup"
TARGET = "nessie.nyc.yellow_taxi_trips"
TARGET_LOCATION = nessie.table_location("gold", "yellow_taxi_trips")

# Silver filtra los IDs PU/DO a [1, 265] y el lookup está completo, así que el left
# join nunca debería dejar una zona en nulo.
MAX_NULL_ZONE_ROWS = 0

# Gold es un contrato con el consumidor: las columnas demostrablemente no nulas llevan NOT NULL.
# Los recargos que dependen del año y el service_zone sin filtrar siguen siendo nullable.
GOLD_SCHEMA = StructType([
    StructField("pickup_datetime", TimestampNTZType(), nullable=False),
    StructField("dropoff_datetime", TimestampNTZType(), nullable=False),
    StructField("pickup_year", IntegerType(), nullable=False),
    StructField("pickup_month", IntegerType(), nullable=False),
    StructField("pickup_week", IntegerType(), nullable=False),
    StructField("pickup_dow", IntegerType(), nullable=False),
    StructField("pickup_hour", IntegerType(), nullable=False),
    StructField("trip_duration_minutes", DoubleType(), nullable=False),
    StructField("pu_location_id", IntegerType(), nullable=False),
    StructField("pu_borough", StringType(), nullable=False),
    StructField("pu_zone", StringType(), nullable=False),
    StructField("pu_service_zone", StringType(), nullable=True),
    StructField("do_location_id", IntegerType(), nullable=False),
    StructField("do_borough", StringType(), nullable=False),
    StructField("do_zone", StringType(), nullable=False),
    StructField("do_service_zone", StringType(), nullable=True),
    StructField("vendor_id", IntegerType(), nullable=False),
    StructField("ratecode_id", IntegerType(), nullable=False),
    StructField("payment_type", LongType(), nullable=False),
    StructField("store_and_fwd_flag", StringType(), nullable=False),
    StructField("passenger_count", DoubleType(), nullable=False),
    StructField("trip_distance", DoubleType(), nullable=False),
    StructField("fare_amount", DoubleType(), nullable=False),
    StructField("tip_amount", DoubleType(), nullable=False),
    StructField("tolls_amount", DoubleType(), nullable=False),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("extra", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True),
    StructField("airport_fee", DoubleType(), nullable=True),
    StructField("cbd_congestion_fee", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=False),
])


def transform(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Renombra a snake_case, deriva las features temporales y desnormaliza la información de zonas."""
    pu_zones = zones.select(
        col("location_id").alias("pu_location_id"),
        col("borough").alias("pu_borough"),
        col("zone").alias("pu_zone"),
        col("service_zone").alias("pu_service_zone"),
    )
    do_zones = zones.select(
        col("location_id").alias("do_location_id"),
        col("borough").alias("do_borough"),
        col("zone").alias("do_zone"),
        col("service_zone").alias("do_service_zone"),
    )

    enriched = (
        trips
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        # Casteamos las columnas tipo entero; silver mantiene ratecode_id como Double por el drift de esquema.
        .withColumn("vendor_id", col("vendor_id").cast("int"))
        .withColumn("ratecode_id", col("ratecode_id").cast("int"))
        .withColumn("pu_location_id", col("pu_location_id").cast("int"))
        .withColumn("do_location_id", col("do_location_id").cast("int"))
        # Features temporales derivadas.
        .withColumn("pickup_year", spark_year("pickup_datetime"))
        .withColumn("pickup_month", spark_month("pickup_datetime"))
        .withColumn("pickup_week", weekofyear("pickup_datetime"))
        # Convención de Python: 0=lun..6=dom (igual que pandas.dt.weekday).
        .withColumn("pickup_dow", weekday("pickup_datetime"))
        .withColumn("pickup_hour", hour("pickup_datetime"))
        .withColumn(
            "trip_duration_minutes",
            (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60.0,
        )
    )

    joined = (
        enriched
        .join(broadcast(pu_zones), on="pu_location_id", how="left")
        .join(broadcast(do_zones), on="do_location_id", how="left")
    )

    # Orden de columnas explícito: gold es un contrato con el consumidor, el esquema debe ser estable.
    return joined.select(
        # Identidad y tiempo
        "pickup_datetime", "dropoff_datetime",
        "pickup_year", "pickup_month", "pickup_week",
        "pickup_dow", "pickup_hour",
        "trip_duration_minutes",
        # Geografía (desnormalizada)
        "pu_location_id", "pu_borough", "pu_zone", "pu_service_zone",
        "do_location_id", "do_borough", "do_zone", "do_service_zone",
        # Atributos del viaje
        "vendor_id", "ratecode_id", "payment_type", "store_and_fwd_flag",
        "passenger_count", "trip_distance",
        # Importes
        "fare_amount", "tip_amount", "tolls_amount", "mta_tax", "extra",
        "improvement_surcharge", "congestion_surcharge",
        "airport_fee", "cbd_congestion_fee",
        "total_amount",
    )


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.table(TARGET)
        return True
    except Exception:
        return False


def validate(silver_count: int, gold_count: int, null_zone_count: int) -> tuple[bool, str]:
    """Paso de auditoría: devuelve (passed, reason). Nunca lanza excepción."""
    if gold_count == 0:
        return False, "gold empty after transform"
    if gold_count != silver_count:
        return False, f"row count mismatch: silver={silver_count:,} gold={gold_count:,}"
    if null_zone_count > MAX_NULL_ZONE_ROWS:
        return False, f"{null_zone_count:,} rows have NULL zone after join"
    return True, f"{gold_count:,} rows, exact silver match, no NULL zones"


def wap_build(
    spark: SparkSession, year_val: int, m: int, branch_name: str
) -> tuple[int, int, bool, str]:
    """Ciclo WAP: rama desde main, lee silver, transforma, escribe, audita y fusiona.

    Devuelve (silver_count, gold_count, passed, reason). Quien llama pone en cuarentena si falla.
    """
    label = f"{year_val}-{m:02d}"

    try:
        nessie.create_branch(spark, branch_name, from_ref="main")

        # Leemos las entradas de silver en el ref de silver.
        nessie.set_spark_branch(spark, "silver")
        zones = spark.table(SOURCE_ZONES)
        trips = spark.table(SOURCE_TRIPS).filter(
            (spark_year(col("tpep_pickup_datetime")) == year_val) &
            (spark_month(col("tpep_pickup_datetime")) == m)
        )
        silver_count = trips.count()
        print(f"  [{label}] Silver: {silver_count:,} rows")

        if silver_count == 0:
            print(f"  [{label}] No silver data for this month, skipping")
            return 0, 0, True, "no silver data"

        # Materializamos antes de cambiar de ref; si no, la escritura volvería a leer silver
        # mientras el catálogo apunta a la rama WAP en main.
        gold_df = transform(trips, zones).cache()
        try:
            # Una sola pasada de agregación: recuento de filas + filas con zona nula (PU o DO).
            stats = gold_df.agg(
                count("*").alias("gold_count"),
                spark_sum(when(
                    col("pu_borough").isNull() | col("do_borough").isNull(), 1
                ).otherwise(0)).alias("null_zone_count"),
            ).first()
            gold_count = stats["gold_count"]
            null_zone_count = stats["null_zone_count"] or 0
            print(f"  [{label}] After transform: {gold_count:,} rows "
                  f"({null_zone_count:,} with NULL zone)")

            nessie.set_spark_branch(spark, branch_name)
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {nessie.NAMESPACE}")

            if not table_exists(spark):
                print(f"  [{label}] Creating table {TARGET}")
                spark.sql(f"""
                    CREATE TABLE {TARGET} (
                        {schema_to_ddl(GOLD_SCHEMA)}
                    ) USING iceberg
                    PARTITIONED BY (months(pickup_datetime))
                    LOCATION '{TARGET_LOCATION}'
                    TBLPROPERTIES (
                        'format-version' = '2',
                        'write.spark.check-nullability' = 'false'
                    )
                """)
            print(f"  [{label}] Appending to {TARGET}")
            gold_df.writeTo(TARGET).append()
        finally:
            gold_df.unpersist()

        passed, reason = validate(silver_count, gold_count, null_zone_count)
        if passed:
            print(f"  [{label}] Audit passed: {reason}")
            nessie.merge_branch(spark, source=branch_name, into="main")
        else:
            print(f"  [{label}] Audit FAILED: {reason}")
        return silver_count, gold_count, passed, reason
    finally:
        nessie.set_spark_branch(spark, "main")


def main():
    spark = get_spark(app_name="gold-build-yellow-taxi-trips")
    nessie.ensure_persistent_branches(spark)

    total_silver = total_gold = processed = 0
    quarantined: list[tuple[str, str, str]] = []  # (etiqueta, review_tag, motivo)

    for year_val in YEARS:
        merged_in_year = 0
        for m in MONTHS:
            label = f"{year_val}-{m:02d}"
            branch_name = nessie.wap_branch_name(
                "gold", "yellow_taxi_trips", year_val, m
            )

            if nessie.ref_exists(spark, branch_name):
                print(f"  Dropping stale WAP branch '{branch_name}'")
                nessie.drop_branch(spark, branch_name)

            print(f"\n  [{label}] WAP build (branch: {branch_name})")
            silver_count, gold_count, passed, reason = wap_build(
                spark, year_val, m, branch_name
            )

            if not passed:
                review_tag = nessie.review_tag_name(
                    "gold", "yellow_taxi_trips", year_val, m
                )
                print(f"  [{label}] Quarantining as tag '{review_tag}'")
                nessie.create_tag(spark, review_tag, from_ref=branch_name)
                nessie.drop_branch(spark, branch_name)
                quarantined.append((label, review_tag, reason))
                continue

            nessie.drop_branch(spark, branch_name)

            if silver_count == 0:
                continue

            total_silver += silver_count
            total_gold += gold_count
            processed += 1
            merged_in_year += 1

        if merged_in_year > 0:
            tag_name = nessie.yearly_tag_name("gold", "yellow_taxi_trips", year_val)
            print(f"  Creating tag '{tag_name}' ({merged_in_year} month(s) merged)")
            nessie.create_tag(spark, tag_name, from_ref="main")

    if processed == 0 and not quarantined:
        print("No months processed. Is silver populated?")
        sys.exit(1)

    print(f"\nDone. {processed} months merged -> {TARGET}")
    print(f"  Silver: {total_silver:,}  Gold: {total_gold:,}")

    if quarantined:
        print(f"\nQuarantined months ({len(quarantined)}) — NOT merged to main:")
        for qlabel, qtag, qreason in quarantined:
            print(f"  [{qlabel}] {qtag}: {qreason}")

    print("\nNessie references after gold ETL:")
    nessie.print_references(spark)
    spark.stop()


if __name__ == "__main__":
    main()
