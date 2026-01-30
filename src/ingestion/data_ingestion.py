"""
Data Lakehouse Platform - Data Ingestion Module
Master's Degree Final Project

This module provides data ingestion capabilities including:
- CSV/Parquet file ingestion
- Data validation
- Schema management
"""

import os
from typing import Optional, Dict, Any
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

logger = logging.getLogger(__name__)


class DataIngestionService:
    """Service for ingesting data into the data lakehouse."""
    
    def __init__(self, spark: SparkSession, catalog_name: str = "nessie"):
        """
        Initialize data ingestion service.
        
        Args:
            spark: SparkSession instance
            catalog_name: Target catalog name
        """
        self.spark = spark
        self.catalog_name = catalog_name
    
    def ingest_csv(
        self,
        file_path: str,
        database_name: str,
        table_name: str,
        schema: Optional[StructType] = None,
        delimiter: str = ",",
        header: bool = True,
        overwrite: bool = False
    ) -> bool:
        """
        Ingest CSV file into Iceberg table.
        
        Args:
            file_path: Path to CSV file (local or S3)
            database_name: Target database
            table_name: Target table
            schema: Optional schema
            delimiter: CSV delimiter
            header: Whether CSV has header
            overwrite: Whether to overwrite existing table
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Ingesting CSV: {file_path} -> {database_name}.{table_name}")
            
            # Read CSV
            reader = (
                self.spark.read
                .option("delimiter", delimiter)
                .option("header", header)
            )
            
            if schema:
                reader = reader.schema(schema)
            
            df = reader.csv(file_path)
            
            # Write to Iceberg table
            mode = "overwrite" if overwrite else "append"
            (
                df.write
                .format("iceberg")
                .mode(mode)
                .option("write.format.default", "parquet")
                .saveAsTable(f"{self.catalog_name}.{database_name}.{table_name}")
            )
            
            logger.info(f"Successfully ingested {df.count()} records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest CSV: {e}")
            return False
    
    def ingest_parquet(
        self,
        file_path: str,
        database_name: str,
        table_name: str,
        overwrite: bool = False
    ) -> bool:
        """
        Ingest Parquet file into Iceberg table.
        
        Args:
            file_path: Path to Parquet file (local or S3)
            database_name: Target database
            table_name: Target table
            overwrite: Whether to overwrite existing table
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Ingesting Parquet: {file_path} -> {database_name}.{table_name}")
            
            # Read Parquet
            df = self.spark.read.parquet(file_path)
            
            # Write to Iceberg table
            mode = "overwrite" if overwrite else "append"
            (
                df.write
                .format("iceberg")
                .mode(mode)
                .option("write.format.default", "parquet")
                .saveAsTable(f"{self.catalog_name}.{database_name}.{table_name}")
            )
            
            logger.info(f"Successfully ingested {df.count()} records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest Parquet: {e}")
            return False
    
    def ingest_from_dataframe(
        self,
        df: DataFrame,
        database_name: str,
        table_name: str,
        overwrite: bool = False
    ) -> bool:
        """
        Ingest from PySpark DataFrame into Iceberg table.
        
        Args:
            df: PySpark DataFrame
            database_name: Target database
            table_name: Target table
            overwrite: Whether to overwrite existing table
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Ingesting DataFrame -> {database_name}.{table_name}")
            
            mode = "overwrite" if overwrite else "append"
            (
                df.write
                .format("iceberg")
                .mode(mode)
                .option("write.format.default", "parquet")
                .saveAsTable(f"{self.catalog_name}.{database_name}.{table_name}")
            )
            
            logger.info(f"Successfully ingested {df.count()} records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest DataFrame: {e}")
            return False
    
    def validate_data_quality(
        self,
        database_name: str,
        table_name: str,
        null_checks: Optional[Dict[str, bool]] = None,
        unique_checks: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Validate data quality of ingested table.
        
        Args:
            database_name: Database name
            table_name: Table name
            null_checks: Dict of column -> should_not_be_null
            unique_checks: List of columns that should be unique
            
        Returns:
            Validation report
        """
        try:
            full_table_name = f"{self.catalog_name}.{database_name}.{table_name}"
            df = self.spark.read.table(full_table_name)
            
            report = {
                "table": full_table_name,
                "total_records": df.count(),
                "null_checks": {},
                "unique_checks": {},
                "is_valid": True
            }
            
            # Null checks
            if null_checks:
                for col, should_not_null in null_checks.items():
                    null_count = df.filter(df[col].isNull()).count()
                    report["null_checks"][col] = {
                        "should_not_be_null": should_not_null,
                        "null_count": null_count,
                        "valid": not (should_not_null and null_count > 0)
                    }
                    if not report["null_checks"][col]["valid"]:
                        report["is_valid"] = False
            
            # Unique checks
            if unique_checks:
                for col in unique_checks:
                    unique_count = df.select(col).distinct().count()
                    total = df.count()
                    report["unique_checks"][col] = {
                        "unique_count": unique_count,
                        "total_count": total,
                        "is_unique": unique_count == total
                    }
                    if not report["unique_checks"][col]["is_unique"]:
                        report["is_valid"] = False
            
            logger.info(f"Data quality validation report: {report}")
            return report
            
        except Exception as e:
            logger.error(f"Failed to validate data quality: {e}")
            return {"error": str(e), "is_valid": False}
