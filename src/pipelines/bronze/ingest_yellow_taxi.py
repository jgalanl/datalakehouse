"""Bronze ingestion — NYC TLC yellow taxi parquet files to Iceberg.

WAP pattern: each month goes on an isolated Nessie branch,
gets validated, then merged to bronze.
"""

import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import URLError

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampNTZType,
)

from common.spark_factory import get_spark
from common.iceberg_schema import schema_to_ddl
from common import nessie_manager as nessie

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEARS = [2023, 2024, 2025]
MONTHS = [1, 2, 3]
TABLE = "nessie.nyc.yellow_taxi"
TABLE_LOCATION = nessie.table_location("bronze", "yellow_taxi")
SCHEMA_REPORT_PATH = Path(__file__).parent / "schema_differences.txt"
DOWNLOAD_DIR = Path("/opt/spark/src/.tmp_ingest")  # shared bind mount

# Bronze stays faithful to the source: every column nullable. Nullability
# tightens in silver, once cleaning rules have actually run.
BRONZE_SCHEMA = StructType([
    StructField("VendorID", LongType(), nullable=True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), nullable=True),
    StructField("passenger_count", DoubleType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("RatecodeID", DoubleType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("PULocationID", LongType(), nullable=True),
    StructField("DOLocationID", LongType(), nullable=True),
    StructField("payment_type", LongType(), nullable=True),
    StructField("fare_amount", DoubleType(), nullable=True),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True),
    StructField("airport_fee", DoubleType(), nullable=True),
])

# New field since Jan 2025 — Iceberg schema evolution via Nessie
CBD_FIELD = StructField("cbd_congestion_fee", DoubleType(), nullable=True)


def build_file_list() -> list[tuple[int, int, str, str]]:
    return [
        (year, m, f"{BASE_URL}/yellow_tripdata_{year}-{m:02d}.parquet",
         f"yellow_tripdata_{year}-{m:02d}.parquet")
        for year in YEARS for m in MONTHS
    ]


def download_one(url: str, name: str) -> Path | None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = DOWNLOAD_DIR / name
    print(f"    {url}")
    try:
        urlretrieve(url, dest)
        print(f"      -> {os.path.getsize(dest) / 1024 / 1024:.1f} MB")
        return dest
    except URLError as e:
        print(f"      FAILED: {e}")
        return None


def compare_schemas(schemas: dict[str, pq.ParquetSchema]) -> str:
    if len(schemas) < 2:
        return "Not enough files to compare schemas.\n"

    names = sorted(schemas)
    ref_name, ref_schema = names[0], schemas[names[0]]
    ref_fields = {f.name: str(f.type) for f in ref_schema}

    lines = [
        "=" * 80,
        "NYC TLC Yellow Taxi — Schema Comparison Report",
        f"Generated: {datetime.now().isoformat()}",
        f"Reference: {ref_name}  |  Files compared: {len(names)}",
        "=" * 80,
        f"\n--- Reference schema ({ref_name}) ---",
    ]
    for i, field in enumerate(ref_schema, 1):
        lines.append(f"  {i:>2}. {field.name:<35} {field.type}")

    any_diff = False
    for name in names[1:]:
        curr = {f.name: str(f.type) for f in schemas[name]}
        added = sorted(set(curr) - set(ref_fields))
        removed = sorted(set(ref_fields) - set(curr))
        changed = sorted(c for c in ref_fields.keys() & curr.keys()
                         if ref_fields[c] != curr[c])
        if not (added or removed or changed):
            continue
        any_diff = True
        lines.append(f"\n--- {name} vs {ref_name} ---")
        for c in added:
            lines.append(f"  + {c:<35} {curr[c]}")
        for c in removed:
            lines.append(f"  - {c:<35} {ref_fields[c]}")
        for c in changed:
            lines.append(f"  ~ {c:<35} {ref_fields[c]} -> {curr[c]}")

    if not any_diff:
        lines.append("\nAll files share the same schema.")
    lines.append("\n" + "=" * 80)
    return "\n".join(lines) + "\n"


def save_schema_report(schemas: dict[str, pq.ParquetSchema]) -> None:
    report = compare_schemas(schemas)
    print(report)
    SCHEMA_REPORT_PATH.write_text(report, encoding="utf-8")
    print(f"  Report saved to {SCHEMA_REPORT_PATH}")


def normalize_df(spark: SparkSession, path: Path) -> DataFrame:
    """Cast a raw parquet file to the canonical schema.

    cbd_congestion_fee is only included when present in the source,
    so Iceberg schema evolution kicks in on the first 2025 file.
    """
    raw = spark.read.parquet(str(path))

    # 2025 files use "Airport_fee" instead of "airport_fee"
    col_map = {c.lower(): c for c in raw.columns}
    if "airport_fee" in col_map and col_map["airport_fee"] != "airport_fee":
        raw = raw.withColumnRenamed(col_map["airport_fee"], "airport_fee")

    has_cbd = "cbd_congestion_fee" in raw.columns
    target_fields = list(BRONZE_SCHEMA.fields) + ([CBD_FIELD] if has_cbd else [])
    for field in target_fields:
        if field.name not in raw.columns:
            raw = raw.withColumn(field.name, lit(None).cast(field.dataType))
    return raw.select([col(f.name).cast(f.dataType) for f in target_fields])


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.table(TABLE)
        return True
    except Exception:
        return False


def validate_ingestion(spark: SparkSession, expected_count: int) -> None:
    table_df = spark.table(TABLE)

    required_cols = {f.name for f in BRONZE_SCHEMA.fields}
    evolved_cols = {CBD_FIELD.name}
    actual_cols = set(table_df.columns)
    missing = required_cols - actual_cols
    extra = actual_cols - required_cols - evolved_cols
    if missing or extra:
        raise ValueError(
            f"Column mismatch. Missing: {missing or 'none'}, extra: {extra or 'none'}"
        )

    actual = table_df.count()
    if actual == 0:
        raise ValueError("Table is empty after ingestion")
    if actual < expected_count:
        raise ValueError(f"Expected >= {expected_count} records, got {actual}")

    print(f"  Validation passed: {actual:,} records, {len(table_df.columns)} columns")


def ingest_month(spark: SparkSession, path: Path, year: int, month: int) -> int:
    label = f"{year}-{month:02d}"
    print(f"\n  [{label}] Reading and normalizing...")
    df = normalize_df(spark, path)
    has_cbd = "cbd_congestion_fee" in df.columns
    count = df.count()
    print(f"  [{label}] {count:,} records")

    if not table_exists(spark):
        print(f"  [{label}] Creating table {TABLE}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {nessie.NAMESPACE}")
        spark.sql(f"""
            CREATE TABLE {TABLE} (
                {schema_to_ddl(BRONZE_SCHEMA)}
            ) USING iceberg
            PARTITIONED BY (months(tpep_pickup_datetime))
            LOCATION '{TABLE_LOCATION}'
            TBLPROPERTIES (
                'format-version' = '2',
                'write.spark.check-nullability' = 'false'
            )
        """)

    if has_cbd and "cbd_congestion_fee" not in spark.table(TABLE).columns:
        print(f"  [{label}] Schema evolution: adding cbd_congestion_fee")
        spark.sql(f"ALTER TABLE {TABLE} ADD COLUMNS (cbd_congestion_fee double)")
    print(f"  [{label}] Appending to {TABLE}")
    df.writeTo(TABLE).append()

    print(f"  [{label}] Done ({count:,} records)")
    return count


def wap_ingest(spark: SparkSession, path: Path, year: int, month: int,
               branch_name: str) -> int:
    """Branch from bronze -> ingest -> validate -> merge back."""
    try:
        nessie.create_branch(spark, branch_name, from_ref="bronze")
        nessie.set_spark_branch(spark, branch_name)
        count = ingest_month(spark, path, year, month)
        validate_ingestion(spark, expected_count=count)
        nessie.merge_branch(spark, source=branch_name, into="bronze")
        return count
    finally:
        nessie.set_spark_branch(spark, "bronze")


def main():
    try:
        spark = get_spark(app_name="bronze-ingest-yellow-taxi")
        nessie.ensure_persistent_branches(spark)

        file_list = build_file_list()
        total_records = 0
        ingested = 0
        schemas = {}

        for year, month, url, name in file_list:
            label = f"{year}-{month:02d}"
            branch_name = nessie.wap_branch_name("bronze", "yellow_taxi", year, month)

            print(f"\n  [{label}] Downloading...")
            path = download_one(url, name)
            if path is None:
                continue

            try:
                schemas[name] = pq.read_schema(str(path))

                if nessie.ref_exists(spark, branch_name):
                    print(f"  Dropping stale WAP branch '{branch_name}'")
                    nessie.drop_branch(spark, branch_name)

                print(f"  Creating WAP branch '{branch_name}' from bronze")
                count = wap_ingest(spark, path, year, month, branch_name)
                nessie.drop_branch(spark, branch_name)

                if month == MONTHS[-1]:
                    tag_name = nessie.yearly_tag_name("bronze", "yellow_taxi", year)
                    print(f"  Creating tag '{tag_name}'")
                    nessie.create_tag(spark, tag_name, from_ref="bronze")

                total_records += count
                ingested += 1
            except Exception:
                print(f"\n  [{label}] Ingestion failed. "
                      f"WAP branch '{branch_name}' preserved for inspection.")
                raise
            finally:
                path.unlink(missing_ok=True)

        if ingested == 0:
            print("No files downloaded. Exiting.")
            sys.exit(1)

        print("\n  Schema comparison report\n")
        save_schema_report(schemas)

        print(f"\nDone. {ingested} months ingested -> {TABLE} ({total_records:,} records)")
        print("\nNessie references after ingestion:")
        nessie.print_references(spark)
        spark.stop()
    finally:
        shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
