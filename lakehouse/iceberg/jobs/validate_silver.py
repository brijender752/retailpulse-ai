from iceberg_session import create_iceberg_spark_session

TABLES = (
    "customers","products","orders","order_items","payments",
    "inventory","website_events","support_tickets","marketing_events"
)

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Validate Silver"
    )
    try:
        for table in TABLES:
            name = f"retailpulse.silver.{table}"
            if not spark.catalog.tableExists(name):
                raise RuntimeError(f"Missing {name}")
            print(f"{name}: {spark.table(name).count():,}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
