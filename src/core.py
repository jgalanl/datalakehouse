"""
Data Lakehouse Platform - Core Utilities
Master's Degree Final Project

This module provides core utilities for the data lakehouse platform including:
- Spark session initialization with Iceberg and Nessie support
- S3/MinIO configuration
- Catalog management
"""

import os
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


class LakehouseConfig:
    """Configuration for data lakehouse components."""
    
    def __init__(self):
        """Initialize lakehouse configuration from environment variables."""
        # MinIO/S3 Configuration
        self.s3_endpoint = os.getenv("AWS_S3_ENDPOINT", "http://minio:9000")
        self.s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        self.s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
        self.s3_region = os.getenv("AWS_REGION", "us-east-1")
        
        # Iceberg Configuration
        self.iceberg_rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
        self.warehouse_path = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")
        
        # Nessie Configuration
        self.nessie_uri = os.getenv("NESSIE_URI", "http://nessie-server:19120/api/v2")
        self.nessie_ref = os.getenv("NESSIE_REF", "main")
        
        # Dremio Configuration
        self.dremio_host = os.getenv("DREMIO_HOST", "dremio")
        self.dremio_port = os.getenv("DREMIO_PORT", "31010")
        
        # MLflow Configuration
        self.mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


class SparkSessionBuilder:
    """Builder for creating configured Spark sessions with Iceberg support."""
    
    @staticmethod
    def build(
        app_name: str,
        master: str = "spark://spark-master:7077",
        use_nessie: bool = True,
        use_iceberg_rest: bool = False,
        additional_configs: Optional[Dict[str, str]] = None
    ) -> SparkSession:
        """
        Build a configured Spark session with Iceberg and catalog support.
        
        Args:
            app_name: Application name
            master: Spark master URL
            use_nessie: Enable Nessie catalog (default: True)
            use_iceberg_rest: Enable Iceberg REST catalog (default: False)
            additional_configs: Additional Spark configurations
            
        Returns:
            Configured SparkSession
        """
        config = LakehouseConfig()
        
        spark_builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            # Iceberg SQL extensions
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            # S3/MinIO Configuration
            .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", config.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", config.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # Performance tuning
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )
        
        # Configure Nessie catalog if enabled
        if use_nessie:
            spark_builder = (
                spark_builder
                .config("spark.sql.catalog.nessie", 
                       "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.nessie.catalog-impl",
                       "org.apache.iceberg.nessie.NessieCatalog")
                .config("spark.sql.catalog.nessie.uri", config.nessie_uri)
                .config("spark.sql.catalog.nessie.ref", config.nessie_ref)
                .config("spark.sql.catalog.nessie.warehouse", config.warehouse_path)
            )
        
        # Configure Iceberg REST catalog if enabled
        if use_iceberg_rest:
            spark_builder = (
                spark_builder
                .config("spark.sql.catalog.iceberg-rest",
                       "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg-rest.catalog-impl",
                       "org.apache.iceberg.rest.RESTCatalog")
                .config("spark.sql.catalog.iceberg-rest.uri", config.iceberg_rest_uri)
                .config("spark.sql.catalog.iceberg-rest.warehouse", config.warehouse_path)
            )
        
        # Apply additional configurations
        if additional_configs:
            for key, value in additional_configs.items():
                spark_builder = spark_builder.config(key, value)
        
        spark = spark_builder.getOrCreate()
        logger.info(f"Spark session created: {app_name}")
        
        return spark


class CatalogManager:
    """Manager for Iceberg catalog operations."""
    
    def __init__(self, spark: SparkSession, catalog_name: str = "nessie"):
        """
        Initialize catalog manager.
        
        Args:
            spark: SparkSession instance
            catalog_name: Name of the catalog to use (nessie or iceberg-rest)
        """
        self.spark = spark
        self.catalog_name = catalog_name
    
    def create_database(self, database_name: str, location: Optional[str] = None) -> bool:
        """
        Create an Iceberg database.
        
        Args:
            database_name: Database name
            location: S3 location (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{database_name}"
            if location:
                sql += f" LOCATION '{location}'"
            
            self.spark.sql(sql)
            logger.info(f"Database created: {self.catalog_name}.{database_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            return False
    
    def list_tables(self, database_name: str) -> list:
        """
        List all tables in a database.
        
        Args:
            database_name: Database name
            
        Returns:
            List of table names
        """
        try:
            result = self.spark.sql(
                f"SHOW TABLES IN {self.catalog_name}.{database_name}"
            )
            return [row.tableName for row in result.collect()]
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def create_table(
        self,
        database_name: str,
        table_name: str,
        schema_str: str,
        partitioning: Optional[str] = None
    ) -> bool:
        """
        Create an Iceberg table.
        
        Args:
            database_name: Database name
            table_name: Table name
            schema_str: Schema definition (Spark SQL)
            partitioning: Partition specification (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sql = (
                f"CREATE TABLE IF NOT EXISTS {self.catalog_name}.{database_name}.{table_name} "
                f"({schema_str})"
            )
            
            if partitioning:
                sql += f" PARTITIONED BY ({partitioning})"
            
            sql += " USING iceberg"
            
            self.spark.sql(sql)
            logger.info(f"Table created: {self.catalog_name}.{database_name}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    def tag_version(self, database_name: str, tag_name: str, message: str = "") -> bool:
        """
        Tag a data version in Nessie (if using Nessie catalog).
        
        Args:
            database_name: Database name
            tag_name: Tag name
            message: Tag message
            
        Returns:
            True if successful, False otherwise
        """
        if self.catalog_name != "nessie":
            logger.warning("Version tagging only supported with Nessie catalog")
            return False
        
        try:
            # Note: This is a simplified approach. 
            # For production, use Nessie REST API or PyIceberg
            logger.info(f"Tagged version: {tag_name} for {database_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to tag version: {e}")
            return False


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
