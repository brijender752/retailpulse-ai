from __future__ import annotations
from dataclasses import dataclass
from iceberg_session import create_iceberg_spark_session

@dataclass(frozen=True)
class Dataset:
    name: str
    source: str
    target: str
    required: bool = True

DATASETS = (
    Dataset("customers", "s3a://retailpulse/streaming/bronze/customers/", "retailpulse.bronze.customers"),
    Dataset("products", "s3a://retailpulse/streaming/bronze/products/", "retailpulse.bronze.products"),
    Dataset("orders", "s3a://retailpulse/streaming/bronze/orders/", "retailpulse.bronze.orders"),
    Dataset("order_items", "s3a://retailpulse/streaming/bronze/order_items/", "retailpulse.bronze.order_items"),
    Dataset("payments", "s3a://retailpulse/streaming/bronze/payments/", "retailpulse.bronze.payments"),
    Dataset("inventory", "s3a://retailpulse/streaming/bronze/inventory/", "retailpulse.bronze.inventory"),
    Dataset("website_events", "s3a://retailpulse/streaming/bronze/website_events/", "retailpulse.bronze.website_events"),
    Dataset("support_tickets", "s3a://retailpulse/streaming/bronze/support_tickets/", "retailpulse.bronze.support_tickets"),
    Dataset("marketing_events", "s3a://retailpulse/streaming/bronze/marketing_events/", "retailpulse.bronze.marketing_events"),
    Dataset("customer_360_recovery", "s3a://retailpulse/streaming/gold/customer_360_recovery/", "retailpulse.gold.customer_360", False),
    Dataset("order_summary", "s3a://retailpulse/streaming/gold/order_summary/", "retailpulse.gold.order_summary", False),
    Dataset("order_payment_summary", "s3a://retailpulse/streaming/gold/order_payment_summary/", "retailpulse.gold.order_payment_summary", False),
    Dataset("product_performance", "s3a://retailpulse/streaming/gold/product_performance/", "retailpulse.gold.product_performance", False),
)

def source_exists(spark, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(conf)
    return bool(fs.exists(p))

def migrate_one(spark, dataset: Dataset):
    if not source_exists(spark, dataset.source):
        if dataset.required:
            raise FileNotFoundError(dataset.source)
        print(f"SKIP optional dataset: {dataset.source}")
        return

    reader = spark.read
    if dataset.target.startswith("retailpulse.bronze."):
        # Flink's Bronze FileSink uses nested time buckets, not Hive partitions.
        # Read finalized Parquet files inside those buckets.
        reader = reader.option("recursiveFileLookup", "true").option(
            "pathGlobFilter", "*.parquet"
        )
    # Freeze the file manifest: Flink can commit more files between Spark's
    # count and write actions. Reading the directory again changes the input.
    discovered = reader.parquet(dataset.source)
    files = discovered.inputFiles()
    if not files:
        raise RuntimeError(f"No finalized Parquet files for {dataset.name}")
    df = reader.parquet(*files)
    src_count = df.count()
    print(f"{dataset.name}: source rows={src_count:,}")

    (
        df.writeTo(dataset.target)
        .using("iceberg")
        .createOrReplace()
    )

    target_count = spark.table(dataset.target).count()
    if target_count != src_count:
        raise RuntimeError(
            f"Count mismatch for {dataset.name}: "
            f"{src_count} != {target_count}"
        )
    print(f"SUCCESS {dataset.target}: {target_count:,}")

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Bootstrap MinIO to Iceberg"
    )
    failures = []
    try:
        for ns in ("bronze", "silver", "gold"):
            spark.sql(
                f"CREATE NAMESPACE IF NOT EXISTS retailpulse.{ns}"
            )

        for dataset in DATASETS:
            try:
                migrate_one(spark, dataset)
            except Exception as exc:
                failures.append(f"{dataset.name}: {exc}")
                print(f"FAILED {dataset.name}: {exc}")

        spark.sql(
            "SHOW TABLES IN retailpulse.bronze"
        ).show(truncate=False)
        spark.sql(
            "SHOW TABLES IN retailpulse.gold"
        ).show(truncate=False)

        if failures:
            raise RuntimeError("\n".join(failures))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
