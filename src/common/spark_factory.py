"""SparkSession factory — all config comes from spark-defaults.conf."""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()
