"""Spark session and configuration utilities"""
from pyspark.sql import SparkSession
import logging


def create_spark_session(app_name: str, enable_legacy_datetime: bool = False) -> SparkSession:
    """
    Create configured Spark session for logistics platform.
    
    Args:
        app_name: Application name for Spark UI
        enable_legacy_datetime: Enable legacy datetime rebasing for historical data
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    
    if enable_legacy_datetime:
        builder = builder \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
    
    return builder.getOrCreate()


def get_logger(name: str) -> logging.Logger:
    """Get configured logger for the platform"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
