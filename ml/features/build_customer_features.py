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
    ACTIVITY_TABLE,
    CUSTOMER_FEATURES_TABLE,
    CUSTOMERS_TABLE,
    ML_SCHEMA,
    ORDERS_TABLE,
    PAYMENTS_TABLE,
)


from common.timestamps import source_timestamp


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Customer Features"
    )

    try:

        spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS
            retailpulse.{ML_SCHEMA}
            """
        )

        customers = (
            spark.table(CUSTOMERS_TABLE)
            .select(
                "customer_id",
                "signup_date",
                "customer_segment",
                "country",
                "state",
            )
            .withColumn("signup_date", source_timestamp("signup_date"))
        )

        orders = (
            spark.table(ORDERS_TABLE)
            .withColumn("order_date", source_timestamp("order_date"))
            .filter(
                ~F.upper(
                    F.col("order_status")
                ).isin(
                    "CANCELLED",
                    "CANCELED",
                )
            )
        )

        order_features = (
            orders
            .groupBy("customer_id")
            .agg(
                F.countDistinct(
                    "order_id"
                ).alias(
                    "total_orders"
                ),

                F.sum(
                    "total_amount"
                ).alias(
                    "lifetime_order_value"
                ),

                F.avg(
                    "total_amount"
                ).alias(
                    "average_order_value"
                ),

                F.max(
                    "order_date"
                ).alias(
                    "last_order_date"
                ),

                F.min(
                    "order_date"
                ).alias(
                    "first_order_date"
                ),
            )
        )

        payments = spark.table(
            PAYMENTS_TABLE
        ).withColumn(
            "transaction_timestamp",
            source_timestamp("transaction_timestamp"),
        )

        payment_features = (
            payments
            .groupBy("customer_id")
            .agg(
                F.sum(
                    "amount"
                ).alias(
                    "lifetime_payments"
                ),

                F.countDistinct(
                    "payment_id"
                ).alias(
                    "payment_count"
                ),

                F.max(
                    "transaction_timestamp"
                ).alias(
                    "last_payment_date"
                ),
            )
        )

        activity = spark.table(
            ACTIVITY_TABLE
        )

        features = (
            customers
            .join(
                order_features,
                "customer_id",
                "left",
            )
            .join(
                payment_features,
                "customer_id",
                "left",
            )
            .join(
                activity,
                "customer_id",
                "left",
            )
        )

        features = (
            features

            .withColumn(
                "feature_date",
                F.current_date(),
            )

            .withColumn(
                "tenure_days",
                F.datediff(
                    F.current_date(),
                    F.to_date(
                        "signup_date"
                    ),
                ),
            )

            .withColumn(
                "days_since_last_order",

                F.when(
                    F.col(
                        "last_order_date"
                    ).isNotNull(),

                    F.datediff(
                        F.current_date(),
                        F.to_date(
                            "last_order_date"
                        ),
                    ),
                ).otherwise(
                    F.lit(9999)
                ),
            )

            .withColumn(
                "days_since_last_payment",

                F.when(
                    F.col(
                        "last_payment_date"
                    ).isNotNull(),

                    F.datediff(
                        F.current_date(),
                        F.to_date(
                            "last_payment_date"
                        ),
                    ),
                ).otherwise(
                    F.lit(9999)
                ),
            )

            .withColumn(
                "payment_gap",

                F.coalesce(
                    F.col(
                        "lifetime_order_value"
                    ),
                    F.lit(0.0),
                )
                -
                F.coalesce(
                    F.col(
                        "lifetime_payments"
                    ),
                    F.lit(0.0),
                ),
            )
        )

        numeric_defaults = {
            "total_orders": 0,
            "lifetime_order_value": 0.0,
            "average_order_value": 0.0,
            "lifetime_payments": 0.0,
            "payment_count": 0,
            "website_event_count": 0,
            "session_count": 0,
            "support_ticket_count": 0,
            "open_support_ticket_count": 0,
            "avg_resolution_minutes": 0.0,
            "marketing_impressions": 0,
            "marketing_clicks": 0,
            "marketing_conversions": 0,
            "marketing_cost": 0.0,
        }

        for column, default in (
            numeric_defaults.items()
        ):

            if column in features.columns:

                features = (
                    features.withColumn(
                        column,

                        F.coalesce(
                            F.col(column),
                            F.lit(default),
                        ),
                    )
                )

        selected_columns = [
            "customer_id",
            "feature_date",
            "customer_segment",
            "country",
            "state",
            "tenure_days",
            "days_since_last_order",
            "days_since_last_payment",
            "total_orders",
            "lifetime_order_value",
            "average_order_value",
            "lifetime_payments",
            "payment_count",
            "payment_gap",
        ]

        optional_columns = [
            "website_event_count",
            "session_count",
            "support_ticket_count",
            "open_support_ticket_count",
            "avg_resolution_minutes",
            "marketing_impressions",
            "marketing_clicks",
            "marketing_conversions",
            "marketing_cost",
        ]

        for column in optional_columns:

            if column in features.columns:
                selected_columns.append(
                    column
                )

        features = features.select(
            *selected_columns
        )

        print(
            "\nCustomer feature schema:"
        )

        features.printSchema()

        print(
            "\nSample features:"
        )

        features.show(
            20,
            truncate=False,
        )

        print(
            "\nWriting:",
            CUSTOMER_FEATURES_TABLE,
        )

        (
            features
            .writeTo(
                CUSTOMER_FEATURES_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        count = features.count()

        print(
            f"\nSUCCESS: {count:,} "
            "customer feature rows created."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
