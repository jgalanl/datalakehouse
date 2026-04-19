"""Silver ETL — clean and conform the NYC TLC taxi zone lookup from bronze.

Small reference table (~265 rows). WAP: branch from silver, read bronze, clean,
validate, merge to silver. Full refresh (no partitioning, single snapshot).
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim

from common.spark_factory import get_spark
from common import nessie_manager as nessie

SOURCE = "nessie.bronze.taxi_zone_lookup"
TARGET = "nessie.silver.taxi_zone_lookup"

VALID_LOCATION_MIN = 1
VALID_LOCATION_MAX = 265

# Bronze has 265 rows; allow small drift but flag large changes.
MIN_EXPECTED_ROWS = 250
MAX_EXPECTED_ROWS = 300


def clean(df: DataFrame) -> DataFrame:
    """Rename to snake_case, trim strings, drop invalid rows, dedupe."""
    return (
        df
        .select(
            col("LocationID").cast("int").alias("location_id"),
            trim(col("Borough")).alias("borough"),
            trim(col("Zone")).alias("zone"),
            trim(col("service_zone")).alias("service_zone"),
        )
        .filter(col("location_id").isNotNull())
        .filter(col("location_id").between(VALID_LOCATION_MIN, VALID_LOCATION_MAX))
        .filter(col("borough").isNotNull() & (col("borough") != ""))
        .filter(col("zone").isNotNull() & (col("zone") != ""))
        .dropDuplicates(["location_id"])
    )


def validate(silver_count: int) -> tuple[bool, str]:
    """Audit step: returns (passed, reason). Never raises."""
    if silver_count == 0:
        return False, "silver empty after cleaning"
    if silver_count < MIN_EXPECTED_ROWS:
        return False, f"only {silver_count} rows (expected >= {MIN_EXPECTED_ROWS})"
    if silver_count > MAX_EXPECTED_ROWS:
        return False, f"{silver_count} rows exceeds upper bound {MAX_EXPECTED_ROWS}"
    return True, f"{silver_count} rows kept"


def main() -> None:
    spark = get_spark(app_name="silver-clean-zone-lookup")
    nessie.ensure_persistent_branches(spark)

    today = date.today()
    branch_name = nessie.etl_branch_name(
        layer="silver", table="taxi_zone_lookup",
        year=today.year, month=today.month,
    )
    tag_name = nessie.etl_tag_name(
        layer="silver", table="taxi_zone_lookup", year=today.year,
    )

    if nessie.ref_exists(spark, branch_name):
        print(f"  Dropping stale ETL branch '{branch_name}'")
        nessie.drop_branch(spark, branch_name)

    print(f"\n  WAP clean (branch: {branch_name})")
    try:
        nessie.create_branch(spark, branch_name, from_ref="silver")

        nessie.set_spark_branch(spark, "bronze")
        bronze_df = spark.table(SOURCE)
        bronze_count = bronze_df.count()
        print(f"  Bronze: {bronze_count:,} rows")

        silver_df = clean(bronze_df).cache()
        try:
            silver_count = silver_df.count()
            print(f"  After cleaning: {silver_count:,} rows")

            nessie.set_spark_branch(spark, branch_name)
            spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

            print(f"  Writing {TARGET}")
            silver_df.writeTo(TARGET) \
                .tableProperty("format-version", "2") \
                .createOrReplace()
        finally:
            silver_df.unpersist()

        passed, reason = validate(silver_count)
        if passed:
            print(f"  Audit passed: {reason}")
            nessie.merge_branch(spark, source=branch_name, into="silver")
            nessie.set_spark_branch(spark, "silver")
            print(f"  Creating tag '{tag_name}'")
            nessie.create_tag(spark, tag_name, from_ref="silver")
            nessie.drop_branch(spark, branch_name)
            print(f"\nDone. Zone lookup written to {TARGET}")
        else:
            review_tag = nessie.review_tag_name(
                "silver", "taxi_zone_lookup", today.year, today.month,
            )
            print(f"  Audit FAILED: {reason}")
            print(f"  Quarantining as tag '{review_tag}'")
            nessie.create_tag(spark, review_tag, from_ref=branch_name)
            nessie.drop_branch(spark, branch_name)
            print(f"\nNOT merged to silver. Inspect the quarantine tag: {review_tag}")
    finally:
        nessie.set_spark_branch(spark, "silver")

    print("\nNessie references after silver ETL:")
    nessie.print_references(spark)
    spark.stop()


if __name__ == "__main__":
    main()
