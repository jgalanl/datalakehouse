"""
POC 1: ACID Ingestion - Data Ingestion Script
==============================================
This script demonstrates initial data ingestion into an Apache Iceberg table
stored in MinIO using Apache Spark.

Steps:
1. Initialize SparkSession with Iceberg configuration
2. Load CSV data from local filesystem
3. Create Iceberg table with appropriate schema
4. Write data to MinIO storage
5. Verify table creation and data count

Author: Data Lakehouse POC
Date: 2026-02-15
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="POC-1-ACID-Ingestion"):
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
    logger.info(f"Spark Version: {spark.version}")
    logger.info(f"Master: {spark.sparkContext.master}")

    return spark


def define_schema():
    """
    Define schema for Amazon Sales data

    Returns:
        StructType: Schema definition
    """
    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("discounted_price", StringType(), True),
        StructField("actual_price", StringType(), True),
        StructField("discount_percentage", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("rating_count", StringType(), True),
        StructField("about_product", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("review_id", StringType(), True),
        StructField("review_title", StringType(), True),
        StructField("review_content", StringType(), True),
        StructField("img_link", StringType(), True),
        StructField("product_link", StringType(), True)
    ])

    return schema


def load_csv_data(spark, file_path):
    """
    Load CSV data from local filesystem

    Args:
        spark: SparkSession
        file_path: Path to CSV file

    Returns:
        DataFrame: Loaded data
    """
    logger.info("Loading CSV Data")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    schema = define_schema()

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(file_path)

    record_count = df.count()

    logger.info(f"Data loaded successfully")
    logger.info(f"File: {file_path}")
    logger.info(f"Records: {record_count}")
    logger.info(f"Columns: {len(df.columns)}")

    logger.info("Schema:")
    df.printSchema()

    logger.info("Sample Data (first 5 rows):")
    df.show(5, truncate=True)

    return df


def create_iceberg_table(spark, df, catalog_name, database_name, table_name):
    """
    Create Iceberg table and write data

    Args:
        spark: SparkSession
        df: DataFrame with data
        catalog_name: Iceberg catalog name
        database_name: Database/namespace name
        table_name: Table name

    Returns:
        str: Full table identifier
    """
    logger.info("Creating Iceberg Table")

    # Create database if not exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database_name}")
    logger.info(f"Namespace created/verified: {catalog_name}.{database_name}")

    # Full table identifier
    full_table_name = f"{catalog_name}.{database_name}.{table_name}"

    # Drop table if exists (for POC purposes)
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    logger.info(f"Dropped existing table (if any): {full_table_name}")

    # Write data as Iceberg table
    logger.info(f"Writing data to Iceberg table: {full_table_name}")
    logger.info("This may take a moment...")

    df.writeTo(full_table_name) \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .createOrReplace()

    logger.info(f"Iceberg table created successfully")

    return full_table_name


def verify_table(spark, full_table_name):
    """
    Verify table creation and display metadata

    Args:
        spark: SparkSession
        full_table_name: Full table identifier
    """
    logger.info("Verifying Table")

    # Read back from table
    df_read = spark.table(full_table_name)
    count = df_read.count()

    logger.info(f"Table verified: {full_table_name}")
    logger.info(f"Record count: {count}")

    # Show table properties
    logger.info("Table Properties:")
    spark.sql(f"DESCRIBE EXTENDED {full_table_name}").show(truncate=False)

    # Show table history
    logger.info("Table History:")
    spark.sql(f"SELECT * FROM {full_table_name}.history").show(truncate=False)

    # Show table snapshots
    logger.info("Table Snapshots:")
    spark.sql(f"SELECT * FROM {full_table_name}.snapshots").show(truncate=False)

    # Show sample data
    logger.info("Sample Data from Table (first 10 rows):")
    df_read.select("product_id", "product_name", "category", "discounted_price", "rating").show(10)

    # Show statistics by category
    logger.info("Statistics by Category:")
    df_read.groupBy("category") \
        .agg(
            {"product_id": "count",
             "rating": "avg",
             "discounted_price": "avg"}
        ) \
        .withColumnRenamed("count(product_id)", "product_count") \
        .withColumnRenamed("avg(rating)", "avg_rating") \
        .withColumnRenamed("avg(discounted_price)", "avg_price") \
        .show()


def main():
    """
    Main execution function
    """
    logger.info("=" * 80)
    logger.info("POC 1: ACID INGESTION - DATA INGESTION")
    logger.info("Objective: Ingest data into Apache Iceberg table using Spark")
    logger.info("=" * 80)

    # Configuration
    CSV_FILE = "/opt/spark/src/pocs/1-acid-ingestion/data/amazon.csv"
    CATALOG = "nessie"
    DATABASE = "sales_poc"
    TABLE = "amazon_sales"

    spark = None

    try:
        # Step 1: Create Spark session
        spark = create_spark_session()

        # Step 2: Load CSV data
        df = load_csv_data(spark, CSV_FILE)

        # Step 3: Create Iceberg table
        full_table_name = create_iceberg_table(spark, df, CATALOG, DATABASE, TABLE)

        # Step 4: Verify table
        verify_table(spark, full_table_name)

        logger.info("=" * 80)
        logger.info("Data ingestion completed successfully")
        logger.info(f"Table created: {full_table_name}")
        logger.info("Next step: Run 02_acid_operations.py to perform INSERT, UPDATE, DELETE")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error("Data ingestion failed")
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
