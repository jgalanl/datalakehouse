"""
POC 1: ACID Ingestion - ACID Operations Script
===============================================
This script demonstrates ACID operations (INSERT, UPDATE, DELETE) on an
Apache Iceberg table, proving that a datalakehouse can behave as a data warehouse.

Operations demonstrated:
1. INSERT - Add new records to the table
2. UPDATE - Modify existing records
3. DELETE - Remove records from the table
4. Time Travel - Query historical data

Author: Data Lakehouse POC
Date: 2026-02-15
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="POC-1-ACID-Operations"):
    """
    Create and configure Spark session with Iceberg support

    Args:
        app_name: Name for the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    logger.info("Creating Spark Session with Iceberg Configuration")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie-server:19120/api/v2") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Spark Session created successfully")
    logger.info(f"Application Name: {spark.sparkContext.appName}")

    return spark


def show_table_stats(spark, table_name, operation="Current State"):
    """
    Display current table statistics

    Args:
        spark: SparkSession
        table_name: Full table identifier
        operation: Description of current operation
    """
    logger.info(f"Table Statistics - {operation}")

    df = spark.table(table_name)
    total_count = df.count()

    logger.info(f"Total records: {total_count}")

    # Show distribution by category
    logger.info("Distribution by category:")
    df.groupBy("category").count().orderBy("category").show()

    return total_count


def insert_new_records(spark, table_name):
    """
    Demonstrate INSERT operation by adding new records

    Args:
        spark: SparkSession
        table_name: Full table identifier
    """
    logger.info("=" * 80)
    logger.info("OPERATION 1: INSERT - Adding new records")
    logger.info("=" * 80)

    # Count before insert
    count_before = show_table_stats(spark, table_name, "Before INSERT")

    # Create new records to insert
    new_records = [
        ("P9999", "NEW Wireless Earbuds", "Electronics", "1999", "3999", "50",
         "4.5", "100", "Premium sound quality|30 hour battery|Waterproof",
         "U9999", "Test User", "R9999", "Great product", "Excellent sound quality",
         "https://example.com/img9999.jpg", "https://example.com/product9999"),
        ("P9998", "NEW Smart Watch", "Electronics", "4999", "9999", "50",
         "4.7", "250", "Heart rate monitor|GPS tracking|Water resistant",
         "U9998", "Another User", "R9998", "Amazing watch", "Best purchase ever",
         "https://example.com/img9998.jpg", "https://example.com/product9998"),
        ("P9997", "NEW Laptop Sleeve", "Accessories", "799", "1299", "38",
         "4.3", "180", "Padded protection|Multiple compartments|Durable material",
         "U9997", "Third User", "R9997", "Good quality", "Protects my laptop well",
         "https://example.com/img9997.jpg", "https://example.com/product9997")
    ]

    # Get existing table schema
    df_existing = spark.table(table_name)
    columns = df_existing.columns

    # Create DataFrame with new records
    df_new = spark.createDataFrame(new_records, schema=columns)

    logger.info(f"Inserting {df_new.count()} new records")
    logger.info("New records:")
    df_new.select("product_id", "product_name", "category", "discounted_price").show()

    # Insert using SQL
    df_new.createOrReplaceTempView("new_records")
    spark.sql(f"""
        INSERT INTO {table_name}
        SELECT * FROM new_records
    """)

    # Count after insert
    count_after = show_table_stats(spark, table_name, "After INSERT")

    logger.info(f"Records added: {count_after - count_before}")
    logger.info("INSERT operation completed successfully")


def update_records(spark, table_name):
    """
    Demonstrate UPDATE operation by modifying existing records

    Args:
        spark: SparkSession
        table_name: Full table identifier
    """
    logger.info("=" * 80)
    logger.info("OPERATION 2: UPDATE - Modifying existing records")
    logger.info("=" * 80)

    # Show records before update
    logger.info("Records before UPDATE (filtering Electronics with rating < 4.5):")
    spark.sql(f"""
        SELECT product_id, product_name, category, rating, discount_percentage
        FROM {table_name}
        WHERE category = 'Electronics' AND CAST(rating AS FLOAT) < 4.5
    """).show(10)

    # Perform UPDATE using MERGE
    logger.info("Updating discount_percentage to 60 for Electronics with rating < 4.5")

    spark.sql(f"""
        MERGE INTO {table_name} t
        USING (
            SELECT product_id
            FROM {table_name}
            WHERE category = 'Electronics' AND CAST(rating AS FLOAT) < 4.5
        ) s
        ON t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET
            t.discount_percentage = '60'
    """)

    # Show records after update
    logger.info("Records after UPDATE:")
    spark.sql(f"""
        SELECT product_id, product_name, category, rating, discount_percentage
        FROM {table_name}
        WHERE category = 'Electronics' AND discount_percentage = '60'
    """).show(10)

    logger.info("UPDATE operation completed successfully")


def delete_records(spark, table_name):
    """
    Demonstrate DELETE operation by removing records

    Args:
        spark: SparkSession
        table_name: Full table identifier
    """
    logger.info("=" * 80)
    logger.info("OPERATION 3: DELETE - Removing records")
    logger.info("=" * 80)

    # Count before delete
    count_before = show_table_stats(spark, table_name, "Before DELETE")

    # Show records to be deleted
    logger.info("Records to be deleted (product_id starting with 'P999'):")
    spark.sql(f"""
        SELECT product_id, product_name, category
        FROM {table_name}
        WHERE product_id LIKE 'P999%'
    """).show()

    # Perform DELETE
    logger.info("Deleting records where product_id starts with 'P999'")

    spark.sql(f"""
        DELETE FROM {table_name}
        WHERE product_id LIKE 'P999%'
    """)

    # Count after delete
    count_after = show_table_stats(spark, table_name, "After DELETE")

    logger.info(f"Records deleted: {count_before - count_after}")
    logger.info("DELETE operation completed successfully")


def demonstrate_time_travel(spark, table_name):
    """
    Demonstrate time travel capabilities of Iceberg

    Args:
        spark: SparkSession
        table_name: Full table identifier
    """
    logger.info("=" * 80)
    logger.info("OPERATION 4: TIME TRAVEL - Query historical data")
    logger.info("=" * 80)

    # Show all snapshots
    logger.info("Table snapshots (history):")
    snapshots_df = spark.sql(f"SELECT * FROM {table_name}.snapshots")
    snapshots_df.show(truncate=False)

    # Get snapshot IDs
    snapshots = snapshots_df.collect()

    if len(snapshots) >= 2:
        # Query using snapshot (before DELETE)
        snapshot_id = snapshots[-2].snapshot_id
        logger.info(f"Querying data at snapshot: {snapshot_id}")

        df_historical = spark.read \
            .option("snapshot-id", snapshot_id) \
            .format("iceberg") \
            .load(table_name)

        count_historical = df_historical.count()
        logger.info(f"Records in historical snapshot: {count_historical}")

        # Show the deleted records that exist in historical snapshot
        logger.info("Records that were deleted (visible in historical snapshot):")
        df_historical.filter(col("product_id").like("P999%")) \
            .select("product_id", "product_name", "category") \
            .show()

    # Show table history
    logger.info("Table history:")
    spark.sql(f"SELECT * FROM {table_name}.history").show(truncate=False)

    logger.info("TIME TRAVEL demonstration completed successfully")


def show_table_metadata(spark, table_name):
    """
    Display detailed table metadata

    Args:
        spark: SparkSession
        table_name: Full table identifier
    """
    logger.info("=" * 80)
    logger.info("TABLE METADATA")
    logger.info("=" * 80)

    # Table properties
    logger.info("Table Properties:")
    spark.sql(f"DESCRIBE EXTENDED {table_name}").show(truncate=False)

    # File statistics
    logger.info("File Statistics:")
    spark.sql(f"SELECT * FROM {table_name}.files").show(truncate=False)

    # Partition information
    logger.info("Partition Information:")
    spark.sql(f"SELECT * FROM {table_name}.partitions").show(truncate=False)


def main():
    """
    Main execution function
    """
    logger.info("=" * 80)
    logger.info("POC 1: ACID INGESTION - ACID OPERATIONS")
    logger.info("Objective: Demonstrate ACID operations on Iceberg tables")
    logger.info("=" * 80)

    # Configuration
    CATALOG = "nessie"
    DATABASE = "sales_poc"
    TABLE = "amazon_sales"
    FULL_TABLE_NAME = f"{CATALOG}.{DATABASE}.{TABLE}"

    spark = None

    try:
        # Create Spark session
        spark = create_spark_session()

        # Verify table exists
        logger.info(f"Verifying table exists: {FULL_TABLE_NAME}")
        spark.table(FULL_TABLE_NAME).limit(1).count()
        logger.info("Table verified successfully")

        # Show initial state
        show_table_stats(spark, FULL_TABLE_NAME, "Initial State")

        # Operation 1: INSERT
        insert_new_records(spark, FULL_TABLE_NAME)

        # Operation 2: UPDATE
        update_records(spark, FULL_TABLE_NAME)

        # Operation 3: DELETE
        delete_records(spark, FULL_TABLE_NAME)

        # Operation 4: TIME TRAVEL
        demonstrate_time_travel(spark, FULL_TABLE_NAME)

        # Show final metadata
        show_table_metadata(spark, FULL_TABLE_NAME)

        logger.info("=" * 80)
        logger.info("All ACID operations completed successfully")
        logger.info("=" * 80)
        logger.info("Summary:")
        logger.info("- INSERT: Added new records to the table")
        logger.info("- UPDATE: Modified existing records using MERGE")
        logger.info("- DELETE: Removed specific records")
        logger.info("- TIME TRAVEL: Queried historical snapshots")
        logger.info("=" * 80)
        logger.info("Conclusion: Datalakehouse successfully behaves as a data warehouse")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error("ACID operations failed")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 80)
        import traceback
        traceback.print_exc()
        return 1

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    sys.exit(main())
