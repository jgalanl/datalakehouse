"""
Data Lakehouse Platform - Example Data Processing Job
Master's Degree Final Project

This script demonstrates a complete ETL pipeline using the data lakehouse.
"""

from src.core import SparkSessionBuilder, CatalogManager
from src.ingestion.data_ingestion import DataIngestionService
from pyspark.sql.functions import col, upper, when, year, month
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main ETL pipeline."""
    
    logger.info("Starting ETL pipeline")
    
    # ========================================================================
    # 1. Initialize Spark and Catalogs
    # ========================================================================
    spark = SparkSessionBuilder.build(
        app_name="etl_pipeline",
        master="spark://spark-master:7077",
        use_nessie=True
    )
    
    catalog = CatalogManager(spark, catalog_name="nessie")
    ingestion = DataIngestionService(spark, catalog_name="nessie")
    
    # ========================================================================
    # 2. Create Databases
    # ========================================================================
    catalog.create_database("raw", location="s3a://warehouse/raw/")
    catalog.create_database("staging", location="s3a://warehouse/staging/")
    catalog.create_database("analytics", location="s3a://warehouse/analytics/")
    
    logger.info("Created databases: raw, staging, analytics")
    
    # ========================================================================
    # 3. Create and Populate Raw Tables
    # ========================================================================
    # Create sample customers table
    customers_data = [
        (1, "alice@example.com", "2024-01-01"),
        (2, "bob@example.com", "2024-01-02"),
        (3, "charlie@example.com", "2024-01-03"),
    ]
    
    customers_df = spark.createDataFrame(
        customers_data,
        ["customer_id", "email", "signup_date"]
    )
    
    customers_df.write.mode("overwrite").format("iceberg") \
        .option("write.format.default", "parquet") \
        .saveAsTable("nessie.raw.customers")
    
    logger.info("Loaded raw.customers")
    
    # Create sample orders table
    orders_data = [
        (1, 1, 100.0, "2024-01-15"),
        (2, 1, 50.0, "2024-01-20"),
        (3, 2, 200.0, "2024-01-18"),
        (4, 3, 75.0, "2024-01-22"),
    ]
    
    orders_df = spark.createDataFrame(
        orders_data,
        ["order_id", "customer_id", "amount", "order_date"]
    )
    
    orders_df.write.mode("overwrite").format("iceberg") \
        .option("write.format.default", "parquet") \
        .saveAsTable("nessie.raw.orders")
    
    logger.info("Loaded raw.orders")
    
    # ========================================================================
    # 4. Data Transformation and Cleaning
    # ========================================================================
    # Read raw data
    customers = spark.read.table("nessie.raw.customers")
    orders = spark.read.table("nessie.raw.orders")
    
    # Clean and transform customers
    customers_cleaned = customers.select(
        col("customer_id"),
        upper(col("email")).alias("email"),
        col("signup_date").cast("date").alias("signup_date")
    ).filter(col("customer_id").isNotNull())
    
    customers_cleaned.write.mode("overwrite").format("iceberg") \
        .option("write.format.default", "parquet") \
        .saveAsTable("nessie.staging.customers")
    
    logger.info("Transformed customers to staging")
    
    # Clean and transform orders
    orders_cleaned = orders.select(
        col("order_id"),
        col("customer_id"),
        col("amount").cast("decimal(10, 2)").alias("amount"),
        col("order_date").cast("date").alias("order_date")
    ).filter(col("order_id").isNotNull())
    
    orders_cleaned.write.mode("overwrite").format("iceberg") \
        .option("write.format.default", "parquet") \
        .saveAsTable("nessie.staging.orders")
    
    logger.info("Transformed orders to staging")
    
    # ========================================================================
    # 5. Data Aggregation and Analytics
    # ========================================================================
    # Join and aggregate
    customer_metrics = spark.sql("""
        SELECT 
            c.customer_id,
            c.email,
            COUNT(o.order_id) as total_orders,
            SUM(o.amount) as total_spent,
            AVG(o.amount) as avg_order_value,
            MAX(o.order_date) as last_order_date
        FROM nessie.staging.customers c
        LEFT JOIN nessie.staging.orders o 
            ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.email
    """)
    
    customer_metrics.write.mode("overwrite").format("iceberg") \
        .option("write.format.default", "parquet") \
        .saveAsTable("nessie.analytics.customer_metrics")
    
    logger.info("Created customer_metrics analytics table")
    
    # ========================================================================
    # 6. Data Validation
    # ========================================================================
    report = ingestion.validate_data_quality(
        database_name="analytics",
        table_name="customer_metrics",
        null_checks={"customer_id": True, "email": True},
        unique_checks=["customer_id", "email"]
    )
    
    if report["is_valid"]:
        logger.info("✓ Data validation passed")
    else:
        logger.warning("⚠ Data validation failed")
        logger.warning(f"Issues: {report}")
    
    # ========================================================================
    # 7. Generate Summary Report
    # ========================================================================
    summary = spark.sql("""
        SELECT 
            COUNT(DISTINCT customer_id) as total_customers,
            COUNT(*) as rows_in_metrics,
            SUM(total_orders) as total_orders,
            SUM(total_spent) as total_revenue,
            AVG(total_spent) as avg_customer_value
        FROM nessie.analytics.customer_metrics
    """)
    
    logger.info("ETL Pipeline Summary:")
    summary.show()
    
    logger.info("✓ ETL pipeline completed successfully")


if __name__ == "__main__":
    main()
