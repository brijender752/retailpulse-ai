import os
from pathlib import Path
from pyspark.sql import SparkSession

from config.settings import (
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_S3A_ENDPOINT,
)


# Hadoop AWS dependency.
#
# IMPORTANT:
# Default matches Hadoop 3.4.1 in the Spark 4.0.1 image.
# Dependencies are resolved through Ivy2 using:
#
# Override HADOOP_AWS_PACKAGE for a different native Spark installation.
#
HADOOP_AWS_PACKAGE = os.getenv("HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.4.1")


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session configured for:
    
    PostgreSQL JDBC
    +
    MinIO through S3A
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            HADOOP_AWS_PACKAGE,
        )
        .config(
            "spark.jars",
            str(Path(__file__).resolve().parents[1] / "jars" / "postgresql-42.7.12.jar"),
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_S3A_ENDPOINT,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
