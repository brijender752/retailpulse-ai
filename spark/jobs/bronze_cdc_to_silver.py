import os
from pathlib import Path

# PySpark on Windows requires winutils.exe before it starts its Java gateway.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_HADOOP_HOME = PROJECT_ROOT / "tools" / "hadoop"
if (LOCAL_HADOOP_HOME / "bin" / "winutils.exe").exists():
    os.environ["HADOOP_HOME"] = str(LOCAL_HADOOP_HOME)
    os.environ["hadoop.home.dir"] = str(LOCAL_HADOOP_HOME)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BRONZE_BASE = "s3a://retailpulse/bronze"
SILVER_BASE = "s3a://retailpulse/silver"

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.5.0"

# All RetailPulse source tables
TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory",
    "website_events",
    "support_tickets",
    "marketing_events",
]

# ============================================================
# PRIMARY KEYS
# ============================================================

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
    "payments": ["payment_id"],
    "inventory": ["inventory_id"],
    "website_events": ["event_id"],
    "support_tickets": ["ticket_id"],
    "marketing_events": ["marketing_event_id"],
}

# ============================================================
# TIMESTAMP COLUMNS
# ============================================================

TIMESTAMP_COLUMNS = {
    "customers": [
        "signup_date",
        "updated_at",
        "created_at",
    ],
    "products": [
        "created_at",
        "updated_at",
    ],
    "orders": [
        "order_date",
        "created_at",
        "updated_at",
    ],
    "order_items": [],
    "payments": [
        "transaction_timestamp",
    ],
    "inventory": [
        "updated_at",
    ],
    "website_events": [
        "event_timestamp",
    ],
    "support_tickets": [
        "created_at",
    ],
    "marketing_events": [
        "event_timestamp",
    ],
}

# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("RetailPulse - Bronze CDC to Silver")
        # Keep local Spark/MinIO writes bounded on a Windows development host.
        .master("local[2]")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            HADOOP_AWS_PACKAGE,
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT,
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
        # Avoid Hadoop's disk-backed Windows upload path, which requires an
        # unavailable NativeIO Windows library.
        .config(
            "spark.hadoop.fs.s3a.fast.upload.buffer",
            "array",
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# READ BRONZE
# ============================================================

def read_bronze_table(spark, table_name):

    path = f"{BRONZE_BASE}/{table_name}"

    print()
    print("=" * 70)
    print(f"Reading Bronze: {table_name}")
    print(f"Path: {path}")
    print("=" * 70)

    # Flink FileSink writes files under time-bucket directories rather than
    # directly at the table root.  Recursively discover those Parquet parts.
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(path)
    )

    print(f"Bronze rows: {df.count()}")

    return df


# ============================================================
# VALIDATE BRONZE CDC METADATA
# ============================================================

def validate_metadata(df, table_name):

    required_columns = [
        "_op",
        "_ts_ms",
        "_ingested_at",
        "_source_table",
    ]

    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:

        raise ValueError(
            f"{table_name}: Missing Bronze CDC metadata columns: {missing}"
        )

    print(
        f"{table_name}: CDC metadata columns validated"
    )


# ============================================================
# NORMALIZE CDC METADATA
# ============================================================

def normalize_metadata(df, table_name):

    df = (
        df
        .withColumn(
            "_op",
            F.upper(F.trim(F.col("_op")))
        )
        .withColumn(
            "_ts_ms",
            F.col("_ts_ms").cast("long")
        )
        .withColumn(
            "_ingested_at",
            F.to_timestamp(F.col("_ingested_at"))
        )
        .withColumn(
            "_source_table",
            F.trim(F.col("_source_table"))
        )
    )

    # Make sure the source table is correct
    df = df.withColumn(
        "_source_table",
        F.lit(table_name)
    )

    return df


# ============================================================
# VALIDATE CDC OPERATIONS
# ============================================================

def validate_operations(df, table_name):

    invalid_ops = (
        df
        .filter(
            ~F.col("_op").isin(
                "R",
                "C",
                "U",
                "D",
            )
        )
        .select("_op")
        .distinct()
        .collect()
    )

    if invalid_ops:

        invalid_values = [
            row["_op"]
            for row in invalid_ops
        ]

        raise ValueError(
            f"{table_name}: Invalid CDC operations: {invalid_values}"
        )

    print(
        f"{table_name}: CDC operations validated"
    )


# ============================================================
# SHOW CDC DISTRIBUTION
# ============================================================

def show_cdc_distribution(df, table_name):

    print(f"{table_name}: CDC operation distribution")

    (
        df
        .groupBy("_op")
        .count()
        .orderBy("_op")
        .show(truncate=False)
    )


# ============================================================
# DEDUPLICATE CDC EVENTS
# ============================================================

def deduplicate_latest(df, table_name):

    primary_keys = PRIMARY_KEYS[table_name]

    print(
        f"{table_name}: Deduplicating using primary key "
        f"{primary_keys}"
    )

    window = (
        Window
        .partitionBy(
            *[
                F.col(column)
                for column in primary_keys
            ]
        )
        .orderBy(
            F.col("_ts_ms").desc_nulls_last(),
            F.col("_ingested_at").desc_nulls_last(),
        )
    )

    df = (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window)
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )

    return df


# ============================================================
# APPLY DELETE SEMANTICS
# ============================================================

def apply_delete_semantics(df, table_name):

    before_count = df.count()

    df = df.filter(
        F.col("_op") != "D"
    )

    after_count = df.count()

    deleted = before_count - after_count

    print(
        f"{table_name}: Removed {deleted} deleted records"
    )

    return df


# ============================================================
# CLEAN SILVER DATA
# ============================================================

def clean_silver(df, table_name):

    # Remove Bronze CDC metadata.
    bronze_metadata = [
        "_op",
        "_ts_ms",
        "_ingested_at",
        "_source_table",
    ]

    columns_to_drop = [
        column
        for column in bronze_metadata
        if column in df.columns
    ]

    df = df.drop(*columns_to_drop)

    # Add Silver processing timestamp.
    df = df.withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

    return df


# ============================================================
# NORMALIZE TIMESTAMP COLUMNS
# ============================================================

def normalize_timestamps(df, table_name):

    timestamp_columns = TIMESTAMP_COLUMNS.get(
        table_name,
        []
    )

    for column in timestamp_columns:

        if column not in df.columns:
            continue

        # If the column is already timestamp,
        # leave it alone.
        if dict(df.dtypes).get(column) == "timestamp":
            continue

        value = F.trim(F.col(column).cast("string"))

        # Debezium source fields can arrive either as ISO-8601 strings or as
        # epoch milliseconds.  ``try_to_timestamp`` keeps ANSI mode from
        # rejecting the numeric representation before the epoch branch runs.
        df = df.withColumn(
            column,
            F.when(
                value.rlike(r"^[0-9]+$"),
                F.to_timestamp(
                    F.from_unixtime(value.cast("double") / F.lit(1000))
                ),
            ).otherwise(
                F.try_to_timestamp(value)
            ),
        )

    return df


# ============================================================
# WRITE SILVER
# ============================================================

def write_silver(df, table_name):

    output_path = f"{SILVER_BASE}/{table_name}"

    print(
        f"{table_name}: Writing Silver -> {output_path}"
    )

    (
        df
        .coalesce(2)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )


# ============================================================
# PROCESS ONE TABLE
# ============================================================

def process_table(spark, table_name):

    print()
    print("#" * 80)
    print(f"PROCESSING TABLE: {table_name}")
    print("#" * 80)

    # --------------------------------------------------------
    # 1. Read Bronze
    # --------------------------------------------------------

    df = read_bronze_table(
        spark,
        table_name
    )

    # --------------------------------------------------------
    # 2. Validate metadata
    # --------------------------------------------------------

    validate_metadata(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 3. Normalize metadata
    # --------------------------------------------------------

    df = normalize_metadata(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 4. Validate CDC operations
    # --------------------------------------------------------

    validate_operations(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 5. Show CDC distribution
    # --------------------------------------------------------

    show_cdc_distribution(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 6. Normalize timestamps
    # --------------------------------------------------------

    df = normalize_timestamps(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 7. Keep latest event per primary key
    # --------------------------------------------------------

    df = deduplicate_latest(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 8. Apply deletes
    # --------------------------------------------------------

    df = apply_delete_semantics(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 9. Remove Bronze metadata
    # --------------------------------------------------------

    df = clean_silver(
        df,
        table_name
    )

    # --------------------------------------------------------
    # 10. Count final Silver rows
    # --------------------------------------------------------

    silver_count = df.count()

    print(
        f"{table_name}: Final Silver rows = {silver_count}"
    )

    # --------------------------------------------------------
    # 11. Show schema
    # --------------------------------------------------------

    print(
        f"{table_name}: Silver schema"
    )

    df.printSchema()

    # --------------------------------------------------------
    # 12. Write Silver
    # --------------------------------------------------------

    write_silver(
        df,
        table_name
    )

    print(
        f"{table_name}: Silver processing COMPLETE"
    )

    return silver_count


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 80)
    print("RetailPulse AI")
    print("Bronze CDC -> Silver Current State")
    print("=" * 80)

    spark = create_spark_session()

    results = {}

    try:

        for table_name in TABLES:

            try:

                count = process_table(
                    spark,
                    table_name
                )

                results[table_name] = count

            except Exception as exc:

                print()
                print(
                    f"ERROR processing {table_name}: {exc}"
                )

                raise

        # ----------------------------------------------------
        # FINAL SUMMARY
        # ----------------------------------------------------

        print()
        print("=" * 80)
        print("SILVER PIPELINE COMPLETE")
        print("=" * 80)

        for table_name, count in results.items():

            print(
                f"{table_name:<25} {count:>12} rows"
            )

        print("=" * 80)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
