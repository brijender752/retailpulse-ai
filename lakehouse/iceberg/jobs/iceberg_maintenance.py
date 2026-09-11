
from iceberg_session import create_iceberg_spark_session

TABLES = (
    "retailpulse.silver.customers",
    "retailpulse.silver.products",
    "retailpulse.silver.orders",
    "retailpulse.silver.order_items",
    "retailpulse.silver.payments",
    "retailpulse.silver.inventory",
    "retailpulse.silver.website_events",
    "retailpulse.silver.support_tickets",
    "retailpulse.silver.marketing_events",
    "retailpulse.gold.customer_360",
    "retailpulse.gold.order_summary",
    "retailpulse.gold.product_performance",
)

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Iceberg Maintenance"
    )

    try:
        for table in TABLES:
            if not spark.catalog.tableExists(table):
                continue

            spark.sql(f"""
                CALL retailpulse.system.rewrite_data_files(
                    table => '{table}'
                )
            """).show(truncate=False)

            spark.sql(f"""
                CALL retailpulse.system.rewrite_manifests(
                    table => '{table}'
                )
            """).show(truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
