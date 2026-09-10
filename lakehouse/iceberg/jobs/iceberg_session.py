from __future__ import annotations

import os
from pyspark.sql import SparkSession

ICEBERG_VERSION = os.getenv("ICEBERG_VERSION", "1.11.0")
ICEBERG_PACKAGE = (
    "org.apache.iceberg:"
    "iceberg-spark-runtime-4.0_2.13:"
    f"{ICEBERG_VERSION}"
)

HADOOP_AWS_PACKAGE = os.getenv(
    "HADOOP_AWS_PACKAGE",
    "org.apache.hadoop:hadoop-aws:3.4.1",
)

MINIO_ENDPOINT = os.getenv("MINIO_S3A_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
ICEBERG_WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    "s3a://retailpulse/warehouse",
)

def create_iceberg_spark_session(app_name: str) -> SparkSession:
    packages = ",".join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE])

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.retailpulse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.retailpulse.type", "hadoop")
        .config(
            "spark.sql.catalog.retailpulse.warehouse",
            ICEBERG_WAREHOUSE,
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
