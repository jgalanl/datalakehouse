import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg, broadcast, col, count, date_trunc, hour as spark_hour,
    month as spark_month, sum as spark_sum, weekday, weekofyear,
    when, year as spark_year,
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
TARGET = "nessie.nyc.pickup_demand_zone_hour"
TARGET_LOCATION = nessie.table_location("gold", "pickup_demand_zone_hour")

# Silver garantiza IDs PU en [1, 265] y el lookup está completo, así que el left
# join nunca debería dejar una zona en nulo.
MAX_NULL_ZONE_ROWS = 0

# Las claves del group-by, las features temporales y la auditoría hacen que todas las
# columnas sean no nulas salvo service_zone (el zone_lookup de silver lo deja nullable).
GOLD_SCHEMA = StructType([
    StructField("hour_bucket", TimestampNTZType(), nullable=False),
    StructField("pu_location_id", IntegerType(), nullable=False),
    StructField("pu_borough", StringType(), nullable=False),
    StructField("pu_zone", StringType(), nullable=False),
    StructField("pu_service_zone", StringType(), nullable=True),
    StructField("hour_year", IntegerType(), nullable=False),
    StructField("hour_month", IntegerType(), nullable=False),
    StructField("hour_week", IntegerType(), nullable=False),
    StructField("hour_dow", IntegerType(), nullable=False),
    StructField("hour_of_day", IntegerType(), nullable=False),
    StructField("n_pickups", LongType(), nullable=False),
    StructField("total_revenue", DoubleType(), nullable=False),
    StructField("avg_fare", DoubleType(), nullable=False),
    StructField("avg_tip", DoubleType(), nullable=False),
    StructField("avg_distance", DoubleType(), nullable=False),
    StructField("avg_passengers", DoubleType(), nullable=False),
])


def transform(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Agrega los viajes por (pu_location_id, hour_bucket) y desnormaliza la información de zonas."""
    pu_zones = zones.select(
        col("location_id").alias("pu_location_id"),
        col("borough").alias("pu_borough"),
        col("zone").alias("pu_zone"),
        col("service_zone").alias("pu_service_zone"),
    )

    aggregated = (
        trips
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumn("pu_location_id", col("pu_location_id").cast("int"))
        .withColumn("hour_bucket", date_trunc("hour", col("tpep_pickup_datetime")))
        .groupBy("pu_location_id", "hour_bucket")
        .agg(
            count("*").alias("n_pickups"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("fare_amount").alias("avg_fare"),
            avg("tip_amount").alias("avg_tip"),
            avg("trip_distance").alias("avg_distance"),
            avg("passenger_count").alias("avg_passengers"),
        )
        # Features temporales derivadas del propio hour bucket.
        .withColumn("hour_year", spark_year("hour_bucket"))
        .withColumn("hour_month", spark_month("hour_bucket"))
        .withColumn("hour_week", weekofyear("hour_bucket"))
        # Convención de Python: 0=lun..6=dom (igual que pandas.dt.weekday).
        .withColumn("hour_dow", weekday("hour_bucket"))
        .withColumn("hour_of_day", spark_hour("hour_bucket"))
    )

    return (
        aggregated
        .join(broadcast(pu_zones), on="pu_location_id", how="left")
        .select(
            # Clave
            "hour_bucket",
            "pu_location_id", "pu_borough", "pu_zone", "pu_service_zone",
            # Features temporales derivadas
            "hour_year", "hour_month", "hour_week", "hour_dow", "hour_of_day",
            # Variable objetivo + métricas auxiliares
            "n_pickups",
            "total_revenue", "avg_fare", "avg_tip",
            "avg_distance", "avg_passengers",
        )
    )


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.table(TARGET)
        return True
    except Exception:
        return False


def validate(
    silver_count: int, gold_count: int, total_pickups: int, null_zone_count: int,
) -> tuple[bool, str]:
    """Auditoría: la agregación debe conservar el total de viajes y no tener zonas nulas."""
    if gold_count == 0:
        return False, "gold vacío tras la agregación"
    if total_pickups != silver_count:
        return False, (
            f"conservación de pickups rota: silver={silver_count:,} "
            f"sum(n_pickups)={total_pickups:,}"
        )
    if null_zone_count > MAX_NULL_ZONE_ROWS:
        return False, f"{null_zone_count:,} filas con zona NULL tras el join"
    return True, (
        f"{gold_count:,} grupos (zona, hora), {total_pickups:,} pickups conservados"
    )


def wap_aggregate(
    spark: SparkSession, year_val: int, m: int, branch_name: str
) -> tuple[int, int, bool, str]:
    """Ciclo WAP para un (año, mes). Rama desde main → agrega silver → audita."""
    label = f"{year_val}-{m:02d}"

    try:
        nessie.create_branch(spark, branch_name, from_ref="main")

        nessie.set_spark_branch(spark, "silver")
        zones = spark.table(SOURCE_ZONES)
        trips = spark.table(SOURCE_TRIPS).filter(
            (spark_year(col("tpep_pickup_datetime")) == year_val) &
            (spark_month(col("tpep_pickup_datetime")) == m)
        )
        silver_count = trips.count()
        print(f"  [{label}] Silver: {silver_count:,} filas")

        if silver_count == 0:
            print(f"  [{label}] Sin datos silver este mes, se omite")
            return 0, 0, True, "sin datos silver"

        gold_df = transform(trips, zones).cache()
        try:
            # Una sola pasada de agregación: recuento de filas, conservación de pickups y filas con zona nula.
            stats = gold_df.agg(
                count("*").alias("gold_count"),
                spark_sum("n_pickups").alias("total_pickups"),
                spark_sum(when(col("pu_borough").isNull(), 1).otherwise(0))
                    .alias("null_zone_count"),
            ).first()
            gold_count = stats["gold_count"]
            total_pickups = stats["total_pickups"] or 0
            null_zone_count = stats["null_zone_count"] or 0
            print(
                f"  [{label}] Tras la agregación: {gold_count:,} grupos (zona, hora), "
                f"{total_pickups:,} pickups, {null_zone_count:,} zona NULL"
            )

            nessie.set_spark_branch(spark, branch_name)
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {nessie.NAMESPACE}")

            if not table_exists(spark):
                print(f"  [{label}] Creando tabla {TARGET}")
                spark.sql(f"""
                    CREATE TABLE {TARGET} (
                        {schema_to_ddl(GOLD_SCHEMA)}
                    ) USING iceberg
                    PARTITIONED BY (months(hour_bucket))
                    LOCATION '{TARGET_LOCATION}'
                    TBLPROPERTIES (
                        'format-version' = '2',
                        'write.spark.check-nullability' = 'false'
                    )
                """)
            print(f"  [{label}] Añadiendo a {TARGET}")
            gold_df.writeTo(TARGET).append()
        finally:
            gold_df.unpersist()

        passed, reason = validate(
            silver_count, gold_count, total_pickups, null_zone_count
        )
        if passed:
            print(f"  [{label}] Auditoría superada: {reason}")
            nessie.merge_branch(spark, source=branch_name, into="main")
        else:
            print(f"  [{label}] Auditoría FALLIDA: {reason}")
        return silver_count, gold_count, passed, reason
    finally:
        nessie.set_spark_branch(spark, "main")


def main():
    spark = get_spark(app_name="gold-aggregate-demand-zone-hour")
    nessie.ensure_persistent_branches(spark)

    total_silver = total_gold = processed = 0
    quarantined: list[tuple[str, str, str]] = []  # (etiqueta, review_tag, motivo)

    for year_val in YEARS:
        merged_in_year = 0
        for m in MONTHS:
            label = f"{year_val}-{m:02d}"
            branch_name = nessie.wap_branch_name(
                "gold", "pickup_demand_zone_hour", year_val, m
            )

            if nessie.ref_exists(spark, branch_name):
                print(f"  Eliminando rama WAP obsoleta '{branch_name}'")
                nessie.drop_branch(spark, branch_name)

            print(f"\n  [{label}] Agregación WAP (rama: {branch_name})")
            silver_count, gold_count, passed, reason = wap_aggregate(
                spark, year_val, m, branch_name
            )

            if not passed:
                review_tag = nessie.review_tag_name(
                    "gold", "pickup_demand_zone_hour", year_val, m
                )
                print(f"  [{label}] Poniendo en cuarentena como tag '{review_tag}'")
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
            tag_name = nessie.yearly_tag_name(
                "gold", "pickup_demand_zone_hour", year_val
            )
            print(f"  Creando tag '{tag_name}' ({merged_in_year} mes(es) fusionado(s))")
            nessie.create_tag(spark, tag_name, from_ref="main")

    if processed == 0 and not quarantined:
        print("No se procesó ningún mes. ¿Está silver poblado?")
        sys.exit(1)

    print(f"\nHecho. {processed} meses fusionados -> {TARGET}")
    print(f"  Viajes silver: {total_silver:,}  Grupos gold (zona, hora): {total_gold:,}")

    if quarantined:
        print(f"\nMeses en cuarentena ({len(quarantined)}) — NO fusionados a main:")
        for qlabel, qtag, qreason in quarantined:
            print(f"  [{qlabel}] {qtag}: {qreason}")

    print("\nReferencias Nessie tras el ETL gold:")
    nessie.print_references(spark)
    spark.stop()


if __name__ == "__main__":
    main()
