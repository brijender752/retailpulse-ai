
from iceberg_session import create_iceberg_spark_session

SILVER = (
    "customers","products","orders","order_items","payments",
    "inventory","website_events","support_tickets","marketing_events"
)

GOLD = (
    "customer_360","order_summary","product_performance"
)

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Validate Incremental Pipeline"
    )

    try:
        for table in SILVER:
            name = f"retailpulse.silver.{table}"
            if not spark.catalog.tableExists(name):
                raise RuntimeError(f"Missing {name}")
            print(f"{name}: {spark.table(name).count():,}")

        for table in GOLD:
            name = f"retailpulse.gold.{table}"
            if not spark.catalog.tableExists(name):
                raise RuntimeError(f"Missing {name}")
            count = spark.table(name).count()
            if count < 1:
                raise RuntimeError(f"{name} is empty")
            print(f"{name}: {count:,}")

        wm = "retailpulse.control.cdc_watermarks"
        if not spark.catalog.tableExists(wm):
            raise RuntimeError("Watermark table missing")

        spark.table(wm).orderBy("table_name").show(truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
