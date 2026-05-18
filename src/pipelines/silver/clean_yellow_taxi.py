"""Silver ETL — clean and conform NYC TLC yellow taxi trips from bronze.

WAP pattern: branch from silver, read bronze, clean, validate, merge to silver.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, month as spark_month, year as spark_year
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampNTZType,
)

from common.spark_factory import get_spark
from common.iceberg_schema import schema_to_ddl
from common import nessie_manager as nessie

YEARS = [2019, 2020, 2022, 2023, 2025]
MONTHS = [1, 2, 3]
# SOURCE and TARGET share an identifier — same table name, different Nessie
# branch (bronze vs silver). The active ref decides which one a read hits.
SOURCE = "nessie.nyc.yellow_taxi"
TARGET = "nessie.nyc.yellow_taxi"
TARGET_LOCATION = nessie.table_location("silver", "yellow_taxi")

VALID_LOCATION_MIN = 1
VALID_LOCATION_MAX = 265  # 264/265 = Unknown/NV, kept intentionally

# TLC data dictionary (2024 edition)
VALID_VENDOR_IDS    = [1, 2, 6, 7]           # Creative Mobile Tech, Curb Mobility, Myle Technologies, Helix
VALID_RATE_CODES    = [1, 2, 3, 4, 5, 6, 99] # Standard, JFK, Newark, Nassau/Westchester, Negotiated, Group, Null/unknown
VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]  # Flex Fare, Credit card, Cash, No charge, Dispute, Unknown, Voided

MAX_DROP_RATIO = 0.20

# NOT NULL on every column the clean() function provably filters: each filter
# (isNotNull, between, isin, comparison) treats NULL as falsy and drops the
# row, so survivors are guaranteed non-null. Unfiltered surcharges and the
# year-dependent fields (added in 2019/2020/2025) stay nullable.
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


def clean(df: DataFrame) -> DataFrame:
    """Apply silver cleaning rules to a bronze DataFrame slice."""
    return (
        df
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime").isNotNull())
        .filter(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
        .dropDuplicates()
        .filter(col("trip_distance") >= 0)
        .filter(col("fare_amount") >= 0)
        .filter(col("total_amount") >= 0)
        .filter(col("tip_amount") >= 0)
        .filter(col("tolls_amount") >= 0)
        .filter(col("PULocationID").between(VALID_LOCATION_MIN, VALID_LOCATION_MAX))
        .filter(col("DOLocationID").between(VALID_LOCATION_MIN, VALID_LOCATION_MAX))
        .filter(col("VendorID").isin(VALID_VENDOR_IDS))
        # RatecodeID is DoubleType (schema drift); isin() handles int-to-double comparison
        .filter(col("RatecodeID").isin(VALID_RATE_CODES))
        .filter(col("payment_type").isin(VALID_PAYMENT_TYPES))
        .filter(col("store_and_fwd_flag").isin("Y", "N"))
        .filter(col("passenger_count").between(1, 6))
    )


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.table(TARGET)
        return True
    except Exception:
        return False


def validate(bronze_count: int, silver_count: int) -> tuple[bool, str]:
    """Audit step: returns (passed, reason). Never raises — WAP decides the next step."""
    if silver_count == 0:
        return False, "silver empty after cleaning"
    drop_ratio = (bronze_count - silver_count) / bronze_count
    if drop_ratio > MAX_DROP_RATIO:
        return False, (
            f"drop ratio {drop_ratio:.1%} exceeds threshold {MAX_DROP_RATIO:.0%} "
            f"({bronze_count:,} -> {silver_count:,})"
        )
    return True, (
        f"{silver_count:,} kept, {bronze_count - silver_count:,} dropped "
        f"({drop_ratio:.1%})"
    )


def wap_clean(
    spark: SparkSession, year_val: int, m: int, branch_name: str
) -> tuple[int, int, bool, str]:
    """WAP cycle: Write to WAP branch → Audit → Publish (merge) or reject.

    Returns (bronze_count, silver_count, passed, reason). Never merges on fail —
    the caller is responsible for quarantining the WAP branch.
    """
    label = f"{year_val}-{m:02d}"

    try:
        nessie.create_branch(spark, branch_name, from_ref="silver")

        nessie.set_spark_branch(spark, "bronze")
        bronze_df = spark.table(SOURCE).filter(
            (spark_year(col("tpep_pickup_datetime")) == year_val) &
            (spark_month(col("tpep_pickup_datetime")) == m)
        )
        bronze_count = bronze_df.count()
        print(f"  [{label}] Bronze: {bronze_count:,} rows")

        if bronze_count == 0:
            print(f"  [{label}] No bronze data for this month, skipping")
            return 0, 0, True, "no bronze data"

        # Cache to avoid rescanning bronze + re-applying clean filters for the write.
        silver_df = clean(bronze_df).cache()
        try:
            silver_count = silver_df.count()
            print(f"  [{label}] After cleaning: {silver_count:,} rows")

            nessie.set_spark_branch(spark, branch_name)

            if not table_exists(spark):
                print(f"  [{label}] Creating table {TARGET}")
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
            print(f"  [{label}] Appending to {TARGET}")
            silver_df.writeTo(TARGET).append()
        finally:
            silver_df.unpersist()

        passed, reason = validate(bronze_count, silver_count)
        if passed:
            print(f"  [{label}] Audit passed: {reason}")
            nessie.merge_branch(spark, source=branch_name, into="silver")
        else:
            print(f"  [{label}] Audit FAILED: {reason}")
        return bronze_count, silver_count, passed, reason
    finally:
        nessie.set_spark_branch(spark, "silver")


def main():
    spark = get_spark(app_name="silver-clean-yellow-taxi")
    nessie.ensure_persistent_branches(spark)

    total_bronze = total_silver = processed = 0
    quarantined: list[tuple[str, str, str]] = []  # (label, review_tag, reason)

    for year_val in YEARS:
        merged_in_year = 0
        for m in MONTHS:
            label = f"{year_val}-{m:02d}"
            branch_name = nessie.wap_branch_name("silver", "yellow_taxi", year_val, m)

            if nessie.ref_exists(spark, branch_name):
                print(f"  Dropping stale WAP branch '{branch_name}'")
                nessie.drop_branch(spark, branch_name)

            print(f"\n  [{label}] WAP clean (branch: {branch_name})")
            bronze_count, silver_count, passed, reason = wap_clean(
                spark, year_val, m, branch_name
            )

            if not passed:
                # Quarantine: freeze the failed-audit snapshot as an immutable
                # review tag, then drop the WAP branch. Keep going.
                review_tag = nessie.review_tag_name("silver", "yellow_taxi", year_val, m)
                print(f"  [{label}] Quarantining as tag '{review_tag}'")
                nessie.create_tag(spark, review_tag, from_ref=branch_name)
                nessie.drop_branch(spark, branch_name)
                quarantined.append((label, review_tag, reason))
                continue

            # Passed (or empty month): drop the WAP branch
            nessie.drop_branch(spark, branch_name)

            if bronze_count == 0:
                continue

            total_bronze += bronze_count
            total_silver += silver_count
            processed += 1
            merged_in_year += 1

        # Tag the year if at least one month was merged, even when later
        # months were quarantined — the tag still marks a real silver snapshot.
        if merged_in_year > 0:
            tag_name = nessie.yearly_tag_name("silver", "yellow_taxi", year_val)
            print(f"  Creating tag '{tag_name}' ({merged_in_year} month(s) merged)")
            nessie.create_tag(spark, tag_name, from_ref="silver")

    if processed == 0 and not quarantined:
        print("No months processed. Is bronze populated?")
        sys.exit(1)

    cleaned = total_bronze - total_silver
    retention = total_silver / total_bronze if total_bronze > 0 else 0
    print(f"\nDone. {processed} months merged -> {TARGET}")
    print(f"  Bronze: {total_bronze:,}  Silver: {total_silver:,}  "
          f"Dropped: {cleaned:,}  Retention: {retention:.1%}")

    if quarantined:
        print(f"\nQuarantined months ({len(quarantined)}) — NOT merged to silver:")
        for qlabel, qtag, qreason in quarantined:
            print(f"  [{qlabel}] {qtag}: {qreason}")

    print("\nNessie references after silver ETL:")
    nessie.print_references(spark)
    spark.stop()


if __name__ == "__main__":
    main()
