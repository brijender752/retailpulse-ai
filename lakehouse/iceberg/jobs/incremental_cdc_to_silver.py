
from __future__ import annotations
from dataclasses import dataclass
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from iceberg_session import create_iceberg_spark_session

CATALOG = "retailpulse"
CONTROL_NAMESPACE = f"{CATALOG}.control"
WATERMARK_TABLE = f"{CONTROL_NAMESPACE}.cdc_watermarks"
LOOKBACK_MS = 5 * 60 * 1000

@dataclass(frozen=True)
class Entity:
    table: str
    key: str

ENTITIES = (
    Entity("customers", "customer_id"),
    Entity("products", "product_id"),
    Entity("orders", "order_id"),
    Entity("order_items", "order_item_id"),
    Entity("payments", "payment_id"),
    Entity("inventory", "inventory_id"),
    Entity("website_events", "event_id"),
    Entity("support_tickets", "ticket_id"),
    Entity("marketing_events", "marketing_event_id"),
)

def ensure_control_objects(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CONTROL_NAMESPACE}")

    if not spark.catalog.tableExists(WATERMARK_TABLE):
        schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("last_ts_ms", LongType(), False),
            StructField("updated_at", TimestampType(), False),
        ])
        empty_df = spark.createDataFrame([], schema)
        empty_df.writeTo(WATERMARK_TABLE).using("iceberg").create()

def get_watermark(spark, table_name):
    ensure_control_objects(spark)
    rows = (
        spark.table(WATERMARK_TABLE)
        .filter(F.col("table_name") == table_name)
        .select("last_ts_ms")
        .limit(1)
        .collect()
    )
    return int(rows[0]["last_ts_ms"]) if rows else 0

def update_watermark(spark, table_name, new_ts_ms):
    update_df = (
        spark.createDataFrame(
            [(table_name, int(new_ts_ms))],
            ["table_name", "last_ts_ms"],
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    update_df.createOrReplaceTempView("watermark_update")

    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} t
        USING watermark_update s
          ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
          t.last_ts_ms = s.last_ts_ms,
          t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
          table_name, last_ts_ms, updated_at
        )
        VALUES (
          s.table_name, s.last_ts_ms, s.updated_at
        )
    """)

def latest_per_key(df, key):
    ordering = [F.col("_ts_ms").desc_nulls_last()]

    if "_ingested_at" in df.columns:
        ordering.append(F.col("_ingested_at").desc_nulls_last())

    w = Window.partitionBy(key).orderBy(*ordering)

    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

def ensure_silver_table(spark, entity, source_df):
    target = f"{CATALOG}.silver.{entity.table}"

    if spark.catalog.tableExists(target):
        return

    initial = (
        latest_per_key(source_df, entity.key)
        .filter(F.col("_op").isin("r", "c", "u"))
    )

    initial.writeTo(target).using("iceberg").create()

def process_entity(spark, entity):
    source_path = f"s3a://retailpulse/streaming/bronze/{entity.table}/"
    target = f"{CATALOG}.silver.{entity.table}"

    watermark = get_watermark(spark, entity.table)
    threshold = max(0, watermark - LOOKBACK_MS)

    # Flink Bronze files live inside time buckets rather than Hive partitions.
    # Discover finalized Parquet files recursively, excluding in-progress files.
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(source_path)
    )

    required = {entity.key, "_op", "_ts_ms"}
    missing = required - set(df.columns)

    if missing:
        raise RuntimeError(
            f"{entity.table} missing CDC columns: {sorted(missing)}"
        )

    incremental = (
        df.filter(
            F.col("_ts_ms").isNotNull()
            & (F.col("_ts_ms") >= F.lit(threshold))
        )
    )

    if incremental.limit(1).count() == 0:
        print(f"{entity.table}: no new events")
        return

    max_ts = int(
        incremental.agg(F.max("_ts_ms").alias("m")).collect()[0]["m"]
    )

    latest = latest_per_key(incremental, entity.key)

    ensure_silver_table(spark, entity, latest)

    view_name = f"cdc_{entity.table}"
    latest.createOrReplaceTempView(view_name)

    target_columns = spark.table(target).columns
    source_columns = set(latest.columns)
    common = [c for c in target_columns if c in source_columns]

    if entity.key not in common or "_ts_ms" not in common:
        raise RuntimeError(
            f"{target} must contain {entity.key} and _ts_ms"
        )

    update_columns = [c for c in common if c != entity.key]

    update_sql = ",\n".join(
        f"t.`{c}` = s.`{c}`" for c in update_columns
    )
    insert_columns = ", ".join(f"`{c}`" for c in common)
    insert_values = ", ".join(f"s.`{c}`" for c in common)

    spark.sql(f"""
        MERGE INTO {target} t
        USING {view_name} s
          ON t.`{entity.key}` = s.`{entity.key}`

        WHEN MATCHED
          AND s._op = 'd'
          AND s._ts_ms >= t._ts_ms
          THEN DELETE

        WHEN MATCHED
          AND s._op IN ('r','c','u')
          AND s._ts_ms >= t._ts_ms
          THEN UPDATE SET
            {update_sql}

        WHEN NOT MATCHED
          AND s._op IN ('r','c','u')
          THEN INSERT ({insert_columns})
          VALUES ({insert_values})
    """)

    update_watermark(spark, entity.table, max_ts)

    print(
        f"{entity.table}: merged "
        f"{latest.count():,} entities, watermark={max_ts}"
    )

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Incremental CDC to Silver"
    )

    failures = []

    try:
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver"
        )
        ensure_control_objects(spark)

        for entity in ENTITIES:
            try:
                process_entity(spark, entity)
            except Exception as exc:
                failures.append(f"{entity.table}: {exc}")
                print(f"FAILED {entity.table}: {exc}")

        print("\nWATERMARKS")
        spark.table(WATERMARK_TABLE).orderBy("table_name").show(truncate=False)

        if failures:
            raise RuntimeError("\n".join(failures))

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
