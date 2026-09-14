from pyspark.sql import DataFrame

from iceberg_session import (
    create_iceberg_spark_session,
)


DATASETS = {

    # ==============================================
    # BRONZE
    # ==============================================

    "customers_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/customers/",

        "target":
        "retailpulse.bronze.customers",
    },

    "products_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/products/",

        "target":
        "retailpulse.bronze.products",
    },

    "orders_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/orders/",

        "target":
        "retailpulse.bronze.orders",
    },

    "order_items_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/order_items/",

        "target":
        "retailpulse.bronze.order_items",
    },

    "payments_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/payments/",

        "target":
        "retailpulse.bronze.payments",
    },

    "inventory_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/inventory/",

        "target":
        "retailpulse.bronze.inventory",
    },

    "website_events_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/website_events/",

        "target":
        "retailpulse.bronze.website_events",
    },

    "support_tickets_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/support_tickets/",

        "target":
        "retailpulse.bronze.support_tickets",
    },

    "marketing_events_bronze": {
        "source":
        "s3a://retailpulse/streaming/bronze/marketing_events/",

        "target":
        "retailpulse.bronze.marketing_events",
    },

    # ==============================================
    # STREAMING GOLD
    # ==============================================

    "customer_360": {
        "source":
        (
            "s3a://retailpulse/"
            "streaming/gold/"
            "customer_360_recovery/"
        ),

        "target":
        "retailpulse.gold.customer_360",
    },

    "order_summary": {
        "source":
        (
            "s3a://retailpulse/"
            "streaming/gold/"
            "order_summary/"
        ),

        "target":
        "retailpulse.gold.order_summary",
    },

    "order_payment_summary": {
        "source":
        (
            "s3a://retailpulse/"
            "streaming/gold/"
            "order_payment_summary/"
        ),

        "target":
        "retailpulse.gold.order_payment_summary",
    },

    "product_performance": {
        "source":
        (
            "s3a://retailpulse/"
            "streaming/gold/"
            "product_performance/"
        ),

        "target":
        "retailpulse.gold.product_performance",
    },
}


def load_parquet(
    spark,
    path: str,
    recursive: bool = False,
) -> DataFrame:

    print(
        f"Reading: {path}"
    )

    reader = spark.read
    if recursive:
        # Flink FileSink buckets are nested directories, not Hive partitions.
        # Only finalized Bronze Parquet files should be migrated.
        reader = reader.option("recursiveFileLookup", "true").option(
            "pathGlobFilter", "*.parquet"
        )
    return reader.parquet(path)


def write_iceberg(
    df: DataFrame,
    table_name: str,
):

    print(
        f"Writing Iceberg table: "
        f"{table_name}"
    )

    (
        df.writeTo(
            table_name
        )
        .using(
            "iceberg"
        )
        .createOrReplace()
    )


def migrate_dataset(
    spark,
    name: str,
    config: dict,
):

    print(
        "\n"
        "=========================================="
    )

    print(
        f"Dataset: {name}"
    )

    print(
        "=========================================="
    )

    df = load_parquet(
        spark,
        config["source"],
        recursive=config["target"].startswith("retailpulse.bronze."),
    )

    print(
        f"Rows: {df.count():,}"
    )

    df.printSchema()

    write_iceberg(
        df,
        config["target"],
    )

    print(
        f"Completed: {name}"
    )


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse - MinIO to Iceberg"
    )

    # ----------------------------------------------
    # Ensure namespaces
    # ----------------------------------------------

    for namespace in [
        "bronze",
        "silver",
        "gold",
    ]:

        spark.sql(
            f"""
            CREATE NAMESPACE
            IF NOT EXISTS
            retailpulse.{namespace}
            """
        )

    # ----------------------------------------------
    # Migrate
    # ----------------------------------------------

    failures = []

    for (
        name,
        config,
    ) in DATASETS.items():

        try:

            migrate_dataset(
                spark,
                name,
                config,
            )

        except Exception as exc:

            print(
                f"FAILED: {name}"
            )

            print(
                str(exc)
            )

            failures.append(
                name
            )

    # ----------------------------------------------
    # Show resulting tables
    # ----------------------------------------------

    print(
        "\nBRONZE TABLES"
    )

    spark.sql(
        """
        SHOW TABLES
        IN retailpulse.bronze
        """
    ).show(
        truncate=False
    )

    print(
        "\nGOLD TABLES"
    )

    spark.sql(
        """
        SHOW TABLES
        IN retailpulse.gold
        """
    ).show(
        truncate=False
    )

    spark.stop()

    if failures:

        raise RuntimeError(
            "Iceberg migration failed "
            f"for: {failures}"
        )


if __name__ == "__main__":
    main()
