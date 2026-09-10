from __future__ import annotations
from dataclasses import dataclass
from pyspark.sql import Window
from pyspark.sql import functions as F
from iceberg_session import create_iceberg_spark_session

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

def latest_events(df, key):
    if "_op" not in df.columns or "_ts_ms" not in df.columns:
        raise RuntimeError("Bronze CDC requires _op and _ts_ms")

    w = Window.partitionBy(key).orderBy(
        F.col("_ts_ms").desc_nulls_last()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

def ensure_table(spark, df, entity):
    target = f"retailpulse.silver.{entity.table}"
    if spark.catalog.tableExists(target):
        return

    initial = (
        latest_events(df, entity.key)
        .filter(F.col("_op").isin("r", "c", "u"))
    )
    initial.writeTo(target).using("iceberg").create()

def merge_entity(spark, entity):
    bronze = f"retailpulse.bronze.{entity.table}"
    silver = f"retailpulse.silver.{entity.table}"

    df = spark.table(bronze)
    if entity.key not in df.columns:
        raise RuntimeError(f"{bronze} missing {entity.key}")

    latest = latest_events(df, entity.key)
    ensure_table(spark, df, entity)

    view = f"latest_{entity.table}_cdc"
    latest.createOrReplaceTempView(view)

    target_cols = spark.table(silver).columns
    source_cols = set(latest.columns)
    common = [c for c in target_cols if c in source_cols]

    update_cols = [c for c in common if c != entity.key]
    update_sql = ",\n".join(
        f"t.`{c}` = s.`{c}`" for c in update_cols
    )
    insert_cols = ", ".join(f"`{c}`" for c in common)
    insert_vals = ", ".join(f"s.`{c}`" for c in common)

    spark.sql(
        f'''
        MERGE INTO {silver} t
        USING {view} s
          ON t.`{entity.key}` = s.`{entity.key}`
        WHEN MATCHED AND s._op = 'd' THEN DELETE
        WHEN MATCHED AND s._op IN ('r','c','u')
          THEN UPDATE SET {update_sql}
        WHEN NOT MATCHED AND s._op IN ('r','c','u')
          THEN INSERT ({insert_cols})
          VALUES ({insert_vals})
        '''
    )

    print(f"{silver}: {spark.table(silver).count():,}")

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Silver Current State"
    )
    try:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS retailpulse.silver"
        )
        for entity in ENTITIES:
            merge_entity(spark, entity)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
