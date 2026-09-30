from __future__ import annotations

import sys

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)

sys.path.insert(
    0,
    "/opt/retailpulse/ml",
)


from iceberg_session import create_iceberg_spark_session
from common.timestamps import source_timestamp


ORDERS_TABLE = "retailpulse.silver.orders"
ORDER_ITEMS_TABLE = "retailpulse.silver.order_items"
WEBSITE_EVENTS_TABLE = "retailpulse.silver.website_events"

OUTPUT_TABLE = "retailpulse.ml.customer_product_events"


PURCHASE_WEIGHT = 5.0
PRODUCT_VIEW_WEIGHT = 1.0


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Customer Product Events"
    )

    try:

        print("=" * 70)
        print("BUILD CUSTOMER PRODUCT EVENTS")
        print("=" * 70)

        # ====================================================
        # PURCHASE EVENTS
        # ====================================================

        orders = (
            spark.table(ORDERS_TABLE)
            .select(
                "order_id",
                "customer_id",
                "order_date",
                "status",
            )
            .filter(
                ~F.lower(
                    F.coalesce(
                        F.col("status"),
                        F.lit(""),
                    )
                ).isin(
                    "cancelled",
                    "canceled",
                )
            )
        )

        order_items = (
            spark.table(ORDER_ITEMS_TABLE)
            .select(
                "order_id",
                "product_id",
                "quantity",
            )
        )

        purchases = (
            order_items.alias("oi")
            .join(
                orders.alias("o"),
                "order_id",
                "inner",
            )
            .select(
                F.col("o.customer_id").alias(
                    "customer_id"
                ),

                F.col("oi.product_id").alias(
                    "product_id"
                ),

                F.lit("purchase").alias(
                    "event_type"
                ),

                source_timestamp(
                    "o.order_date"
                ).alias(
                    "event_timestamp"
                ),

                (
                    F.lit(PURCHASE_WEIGHT)
                    *
                    F.coalesce(
                        F.col("oi.quantity").cast(
                            "double"
                        ),
                        F.lit(1.0),
                    )
                ).alias(
                    "interaction_weight"
                ),

                F.lit("orders").alias(
                    "source"
                ),

                F.col("order_id").cast(
                    "string"
                ).alias(
                    "source_event_id"
                ),
            )
        )

        # ====================================================
        # WEBSITE PRODUCT-VIEW EVENTS
        # ====================================================

        website = spark.table(
            WEBSITE_EVENTS_TABLE
        )

        website_columns = set(
            website.columns
        )

        required_website_columns = {
            "customer_id",
            "product_id",
            "event_type",
            "event_timestamp",
        }

        missing = (
            required_website_columns
            -
            website_columns
        )

        if missing:

            print(
                "Website product events skipped."
            )

            print(
                "Missing columns:",
                sorted(missing),
            )

            views = None

        else:

            event_id_column = (
                "event_id"
                if "event_id" in website_columns
                else None
            )

            view_events = (
                website
                .filter(
                    F.lower(
                        F.col("event_type")
                    ).isin(
                        "product_view",
                        "view_product",
                        "product_viewed",
                    )
                )
            )

            if event_id_column:

                source_event = (
                    F.col(event_id_column)
                    .cast("string")
                )

            else:

                source_event = (
                    F.sha2(
                        F.concat_ws(
                            "|",
                            F.col(
                                "customer_id"
                            ).cast("string"),
                            F.col(
                                "product_id"
                            ).cast("string"),
                            F.col(
                                "event_timestamp"
                            ).cast("string"),
                        ),
                        256,
                    )
                )

            views = (
                view_events
                .select(
                    "customer_id",
                    "product_id",

                    F.lit(
                        "product_view"
                    ).alias(
                        "event_type"
                    ),

                    source_timestamp(
                        "event_timestamp"
                    ).alias(
                        "event_timestamp"
                    ),

                    F.lit(
                        PRODUCT_VIEW_WEIGHT
                    ).alias(
                        "interaction_weight"
                    ),

                    F.lit(
                        "website_events"
                    ).alias(
                        "source"
                    ),

                    source_event.alias(
                        "source_event_id"
                    ),
                )
            )

        # ====================================================
        # UNION EVENTS
        # ====================================================

        events = purchases

        if views is not None:

            events = (
                events.unionByName(
                    views
                )
            )

        events = (
            events
            .filter(
                F.col("customer_id").isNotNull()
                &
                F.col("product_id").isNotNull()
                &
                F.col(
                    "event_timestamp"
                ).isNotNull()
            )
            .dropDuplicates(
                [
                    "customer_id",
                    "product_id",
                    "event_type",
                    "event_timestamp",
                    "source",
                    "source_event_id",
                ]
            )
            .withColumn(
                "event_date",
                F.to_date(
                    "event_timestamp"
                ),
            )
            .withColumn(
                "generated_at",
                F.current_timestamp(),
            )
        )

        event_count = events.count()

        if event_count == 0:

            raise RuntimeError(
                "No customer-product events generated."
            )

        print()
        print(
            "Total events:",
            f"{event_count:,}",
        )

        print()

        (
            events
            .groupBy(
                "event_type"
            )
            .agg(
                F.count("*").alias("events"),
                F.countDistinct(
                    "customer_id"
                ).alias("customers"),
                F.countDistinct(
                    "product_id"
                ).alias("products"),
            )
            .show(
                truncate=False
            )
        )

        print("Event date range:")

        (
            events
            .agg(
                F.min("event_date").alias(
                    "first_event"
                ),
                F.max("event_date").alias(
                    "last_event"
                ),
            )
            .show()
        )

        spark.sql(
            """
            CREATE NAMESPACE IF NOT EXISTS
            retailpulse.ml
            """
        )

        (
            events
            .writeTo(
                OUTPUT_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "SUCCESS:",
            OUTPUT_TABLE,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
