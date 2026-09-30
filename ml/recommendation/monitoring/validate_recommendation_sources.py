from __future__ import annotations

import sys

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)

from iceberg_session import create_iceberg_spark_session


REQUIRED_TABLES = [
    "retailpulse.analytics.dim_customer",
    "retailpulse.silver.products",
    "retailpulse.silver.orders",
    "retailpulse.silver.order_items",
    "retailpulse.silver.website_events",
]


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse - Validate Recommendation Sources"
    )

    try:

        print("=" * 70)
        print("VALIDATE RECOMMENDATION SOURCES")
        print("=" * 70)

        for table in REQUIRED_TABLES:

            print()
            print("Checking:", table)

            if not spark.catalog.tableExists(table):

                raise RuntimeError(
                    f"Required table does not exist: {table}"
                )

            df = spark.table(table)

            count = df.count()

            print("Rows:", f"{count:,}")

            if count == 0:

                raise RuntimeError(
                    f"Required table is empty: {table}"
                )

        customers = spark.table(
            "retailpulse.analytics.dim_customer"
        )

        if "customer_id" not in customers.columns:

            raise RuntimeError(
                "dim_customer is missing customer_id"
            )

        products = spark.table(
            "retailpulse.silver.products"
        )

        if "product_id" not in products.columns:

            raise RuntimeError(
                "products is missing product_id"
            )

        orders = spark.table(
            "retailpulse.silver.orders"
        )

        required_orders = {
            "order_id",
            "customer_id",
            "order_date",
        }

        missing = (
            required_orders
            -
            set(orders.columns)
        )

        if missing:

            raise RuntimeError(
                "orders missing columns: "
                + ", ".join(sorted(missing))
            )

        order_items = spark.table(
            "retailpulse.silver.order_items"
        )

        required_items = {
            "order_id",
            "product_id",
        }

        missing = (
            required_items
            -
            set(order_items.columns)
        )

        if missing:

            raise RuntimeError(
                "order_items missing columns: "
                + ", ".join(sorted(missing))
            )

        print()
        print("=" * 70)
        print("SOURCE VALIDATION SUCCESS")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()