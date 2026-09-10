from iceberg_session import create_iceberg_spark_session

REQUIRED = (
    "customers","products","orders","order_items","payments",
    "inventory","website_events","support_tickets","marketing_events"
)

def main():
    spark = create_iceberg_spark_session("RetailPulse - Validate Iceberg")
    try:
        tables = {
            r["tableName"]
            for r in spark.sql(
                "SHOW TABLES IN retailpulse.bronze"
            ).collect()
        }
        missing = [x for x in REQUIRED if x not in tables]
        if missing:
            raise RuntimeError(f"Missing Iceberg tables: {missing}")

        for table in REQUIRED:
            name = f"retailpulse.bronze.{table}"
            count = spark.table(name).count()
            if count < 1:
                raise RuntimeError(f"{name} is empty")
            print(f"{name}: {count:,}")

        spark.sql(
            '''
            SELECT committed_at, snapshot_id, operation
            FROM retailpulse.bronze.customers.snapshots
            ORDER BY committed_at DESC
            '''
        ).show(truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
