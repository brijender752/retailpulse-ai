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


from iceberg_session import (
    create_iceberg_spark_session,
)

from common.ml_config import (
    SILVER_ORDERS_TABLE,
    PRODUCTS_TABLE,
    ORDER_ITEMS_TABLE,
    SILVER_WEBSITE_EVENTS_TABLE,
    RECOMMENDATION_INTERACTIONS_TABLE,
    PURCHASE_WEIGHT,
    PRODUCT_VIEW_WEIGHT,
)
from common.timestamps import source_timestamp


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Customer Product Interactions"
    )

    try:

        print()
        print("=" * 70)
        print("ML-2A CUSTOMER PRODUCT INTERACTIONS")
        print("=" * 70)

        # ====================================================
        # ORDERS
        # ====================================================

        orders = (
            spark.table(
                SILVER_ORDERS_TABLE
            )
            .select(
                "order_id",
                "customer_id",
                source_timestamp(
                    "order_date"
                ).alias(
                    "order_date"
                ),
                "status",
            )
        )

        # Remove cancelled orders.

        orders = (
            orders
            .filter(
                ~F.upper(
                    F.coalesce(
                        F.col("status"),
                        F.lit(""),
                    )
                ).isin(
                    "CANCELLED",
                    "CANCELED",
                )
            )
        )

        # ====================================================
        # ORDER ITEMS
        # ====================================================

        order_items = (
            spark.table(
                ORDER_ITEMS_TABLE
            )
        )

        print()
        print("Order item schema:")

        order_items.printSchema()

        # ====================================================
        # PURCHASE INTERACTIONS
        # ====================================================

        purchase_events = (
            orders.alias("o")
            .join(
                order_items.alias("oi"),
                F.col("o.order_id")
                ==
                F.col("oi.order_id"),
                "inner",
            )
            .select(
                F.col(
                    "o.customer_id"
                ).alias(
                    "customer_id"
                ),

                F.col(
                    "oi.product_id"
                ).alias(
                    "product_id"
                ),

                F.col(
                    "o.order_id"
                ).alias(
                    "order_id"
                ),

                F.col(
                    "o.order_date"
                ).alias(
                    "event_timestamp"
                ),

                F.coalesce(
                    F.col(
                        "oi.quantity"
                    ).cast("double"),
                    F.lit(1.0),
                ).alias(
                    "quantity"
                ),
            )
            .filter(
                F.col(
                    "customer_id"
                ).isNotNull()
                &
                F.col(
                    "product_id"
                ).isNotNull()
            )
        )

        purchase_features = (
            purchase_events
            .groupBy(
                "customer_id",
                "product_id",
            )
            .agg(

                F.countDistinct(
                    "order_id"
                ).alias(
                    "purchase_count"
                ),

                F.sum(
                    "quantity"
                ).alias(
                    "units_purchased"
                ),

                F.max(
                    "event_timestamp"
                ).alias(
                    "last_purchase_at"
                ),
            )
        )

        # ====================================================
        # WEBSITE EVENTS
        # ====================================================

        website = (
            spark.table(
                SILVER_WEBSITE_EVENTS_TABLE
            )
            .select(
                "event_id",
                "customer_id",
                "product_id",
                "event_type",

                source_timestamp(
                    "event_timestamp"
                ).alias(
                    "event_timestamp"
                ),
            )
            .filter(
                F.col(
                    "customer_id"
                ).isNotNull()
                &
                F.col(
                    "product_id"
                ).isNotNull()
            )
        )

        # ====================================================
        # PRODUCT VIEW EVENTS
        # ====================================================

        product_views = (
            website
            .filter(
                F.lower(
                    F.col(
                        "event_type"
                    )
                ).isin(
                    "product_view",
                    "view_product",
                    "product_viewed",
                )
            )
        )

        view_features = (
            product_views
            .groupBy(
                "customer_id",
                "product_id",
            )
            .agg(

                F.countDistinct(
                    "event_id"
                ).alias(
                    "product_view_count"
                ),

                F.max(
                    "event_timestamp"
                ).alias(
                    "last_view_at"
                ),
            )
        )

        # ====================================================
        # COMBINE PURCHASE + VIEW SIGNALS
        # ====================================================

        interactions = (
            purchase_features.alias("p")
            .join(
                view_features.alias("v"),
                [
                    "customer_id",
                    "product_id",
                ],
                "full",
            )
        )

        interactions = (
            interactions
            .withColumn(
                "purchase_count",
                F.coalesce(
                    F.col(
                        "purchase_count"
                    ),
                    F.lit(0),
                ),
            )
            .withColumn(
                "units_purchased",
                F.coalesce(
                    F.col(
                        "units_purchased"
                    ),
                    F.lit(0.0),
                ),
            )
            .withColumn(
                "product_view_count",
                F.coalesce(
                    F.col(
                        "product_view_count"
                    ),
                    F.lit(0),
                ),
            )
        )

        # ====================================================
        # INTERACTION SCORE
        # ====================================================

        interactions = (
            interactions
            .withColumn(
                "purchase_score",

                F.col(
                    "purchase_count"
                )
                *
                F.lit(
                    PURCHASE_WEIGHT
                ),
            )
            .withColumn(
                "view_score",

                F.col(
                    "product_view_count"
                )
                *
                F.lit(
                    PRODUCT_VIEW_WEIGHT
                ),
            )
            .withColumn(
                "interaction_score",

                F.col(
                    "purchase_score"
                )
                +
                F.col(
                    "view_score"
                ),
            )
        )

        # ====================================================
        # LAST INTERACTION
        # ====================================================

        interactions = (
            interactions
            .withColumn(
                "last_interaction_at",

                F.greatest(
                    F.col(
                        "last_purchase_at"
                    ),
                    F.col(
                        "last_view_at"
                    ),
                ),
            )
        )

        # ====================================================
        # PRODUCT INFORMATION
        # ====================================================

        products = (
            spark.table(
                PRODUCTS_TABLE
            )
            .select(
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",

                F.col(
                    "price"
                )
                .cast("double")
                .alias(
                    "price"
                ),
            )
        )

        interactions = (
            interactions
            .join(
                products,
                "product_id",
                "left",
            )
        )

        # ====================================================
        # FINAL DATASET
        # ====================================================

        interactions = (
            interactions
            .select(
                "customer_id",
                "product_id",

                "product_name",
                "category",
                "subcategory",
                "brand",
                "price",

                "purchase_count",
                "units_purchased",
                "product_view_count",

                "purchase_score",
                "view_score",
                "interaction_score",

                "last_purchase_at",
                "last_view_at",
                "last_interaction_at",
            )
        )

        # ====================================================
        # VALIDATION
        # ====================================================

        interaction_count = (
            interactions.count()
        )

        customer_count = (
            interactions
            .select(
                "customer_id"
            )
            .distinct()
            .count()
        )

        product_count = (
            interactions
            .select(
                "product_id"
            )
            .distinct()
            .count()
        )

        print()
        print("=" * 70)
        print("INTERACTION STATISTICS")
        print("=" * 70)

        print(
            "Interactions:",
            f"{interaction_count:,}",
        )

        print(
            "Customers:",
            f"{customer_count:,}",
        )

        print(
            "Products:",
            f"{product_count:,}",
        )

        print()

        interactions.show(
            20,
            truncate=False,
        )

        if interaction_count == 0:

            raise RuntimeError(
                "No customer-product interactions "
                "were generated."
            )

        # ====================================================
        # CREATE ML NAMESPACE
        # ====================================================

        spark.sql(
            """
            CREATE NAMESPACE IF NOT EXISTS
            retailpulse.ml
            """
        )

        # ====================================================
        # WRITE ICEBERG
        # ====================================================

        (
            interactions
            .writeTo(
                RECOMMENDATION_INTERACTIONS_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print("=" * 70)
        print("SUCCESS")
        print("=" * 70)

        print(
            RECOMMENDATION_INTERACTIONS_TABLE
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
