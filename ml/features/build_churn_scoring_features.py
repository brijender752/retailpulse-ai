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
    CUSTOMERS_TABLE,
    SILVER_ORDERS_TABLE,
    SILVER_PAYMENTS_TABLE,
    SILVER_WEBSITE_EVENTS_TABLE,
    SILVER_SUPPORT_TICKETS_TABLE,
    SILVER_MARKETING_EVENTS_TABLE,
    CHURN_SCORING_FEATURES_TABLE,
)
from common.timestamps import source_timestamp


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Churn Scoring Features"
    )

    try:

        # ====================================================
        # CUSTOMERS
        # ====================================================

        customers = (
            spark.table(
                CUSTOMERS_TABLE
            )
            .select(
                "customer_id",
                source_timestamp("signup_date").alias("signup_date"),
                "customer_segment",
                "country",
                "state",
            )
        )

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
                F.col(
                    "total_amount"
                )
                .cast("double")
                .alias(
                    "total_amount"
                ),
            )
        )

        orders = orders.filter(
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

        # ====================================================
        # DETERMINE SCORING DATE
        # ====================================================
        #
        # Do NOT blindly use today's date for synthetic data.
        #
        # We score relative to the latest date represented
        # in the source data.
        # ====================================================

        max_order_date = (
            orders
            .agg(
                F.max(
                    "order_date"
                ).alias(
                    "max_order_date"
                )
            )
            .first()[
                "max_order_date"
            ]
        )

        if max_order_date is None:

            raise RuntimeError(
                "No order data found."
            )

        scoring_date = (
            max_order_date.date()
        )

        print()
        print("=" * 70)
        print("SCORING DATE")
        print("=" * 70)

        print(
            "Latest order date:",
            scoring_date,
        )

        # ====================================================
        # CUSTOMER BASE
        # ====================================================

        base = (
            customers
            .filter(
                F.to_date(
                    "signup_date"
                )
                <=
                F.lit(
                    scoring_date
                )
            )
            .withColumn(
                "prediction_date",
                F.lit(
                    scoring_date
                ).cast("date"),
            )
            .withColumn(
                "tenure_days",
                F.datediff(
                    F.lit(
                        scoring_date
                    ),
                    F.to_date(
                        "signup_date"
                    ),
                ),
            )
        )

        # ====================================================
        # ORDER FEATURES
        # ====================================================

        o = orders.alias("o")
        b = base.alias("b")

        order_features = (
            b.join(
                o,
                (
                    F.col(
                        "b.customer_id"
                    )
                    ==
                    F.col(
                        "o.customer_id"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "o.order_date"
                        )
                    )
                    <=
                    F.col(
                        "b.prediction_date"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "o.order_date"
                        )
                    )
                    >
                    F.date_sub(
                        F.col(
                            "b.prediction_date"
                        ),
                        180,
                    )
                ),
                "left",
            )
            .groupBy(
                "b.customer_id",
                "b.prediction_date",
            )
            .agg(

                F.max(
                    F.to_date(
                        F.col(
                            "o.order_date"
                        )
                    )
                ).alias(
                    "last_order_date"
                ),

                F.countDistinct(
                    F.when(
                        F.to_date(
                            F.col(
                                "o.order_date"
                            )
                        )
                        >
                        F.date_sub(
                            F.col(
                                "b.prediction_date"
                            ),
                            30,
                        ),
                        F.col(
                            "o.order_id"
                        ),
                    )
                ).alias(
                    "orders_30d"
                ),

                F.countDistinct(
                    F.when(
                        F.to_date(
                            F.col(
                                "o.order_date"
                            )
                        )
                        >
                        F.date_sub(
                            F.col(
                                "b.prediction_date"
                            ),
                            90,
                        ),
                        F.col(
                            "o.order_id"
                        ),
                    )
                ).alias(
                    "orders_90d"
                ),

                F.countDistinct(
                    F.col(
                        "o.order_id"
                    )
                ).alias(
                    "orders_180d"
                ),

                F.sum(
                    F.when(
                        F.to_date(
                            F.col(
                                "o.order_date"
                            )
                        )
                        >
                        F.date_sub(
                            F.col(
                                "b.prediction_date"
                            ),
                            30,
                        ),
                        F.col(
                            "o.total_amount"
                        ),
                    ).otherwise(
                        0.0
                    )
                ).alias(
                    "revenue_30d"
                ),

                F.sum(
                    F.when(
                        F.to_date(
                            F.col(
                                "o.order_date"
                            )
                        )
                        >
                        F.date_sub(
                            F.col(
                                "b.prediction_date"
                            ),
                            90,
                        ),
                        F.col(
                            "o.total_amount"
                        ),
                    ).otherwise(
                        0.0
                    )
                ).alias(
                    "revenue_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col(
                            "o.total_amount"
                        ),
                        F.lit(0.0),
                    )
                ).alias(
                    "revenue_180d"
                ),
            )
        )

        order_features = (
            order_features
            .withColumn(
                "days_since_last_order",
                F.when(
                    F.col(
                        "last_order_date"
                    ).isNotNull(),
                    F.datediff(
                        F.col(
                            "prediction_date"
                        ),
                        F.col(
                            "last_order_date"
                        ),
                    ),
                ).otherwise(
                    F.lit(9999)
                ),
            )
            .withColumn(
                "avg_order_value_90d",
                F.when(
                    F.col(
                        "orders_90d"
                    ) > 0,
                    F.col(
                        "revenue_90d"
                    )
                    /
                    F.col(
                        "orders_90d"
                    ),
                ).otherwise(
                    F.lit(0.0)
                ),
            )
        )

        # ====================================================
        # PAYMENTS
        # ====================================================

        payments = (
            spark.table(
                SILVER_PAYMENTS_TABLE
            )
            .select(
                "payment_id",
                "customer_id",
                source_timestamp(
                    "transaction_timestamp"
                ).alias(
                    "payment_timestamp"
                ),
                F.col(
                    "amount"
                )
                .cast("double")
                .alias(
                    "payment_amount"
                ),
            )
        )

        p = payments.alias("p")

        payment_features = (
            b.join(
                p,
                (
                    F.col(
                        "b.customer_id"
                    )
                    ==
                    F.col(
                        "p.customer_id"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "p.payment_timestamp"
                        )
                    )
                    <=
                    F.col(
                        "b.prediction_date"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "p.payment_timestamp"
                        )
                    )
                    >
                    F.date_sub(
                        F.col(
                            "b.prediction_date"
                        ),
                        90,
                    )
                ),
                "left",
            )
            .groupBy(
                "b.customer_id"
            )
            .agg(
                F.countDistinct(
                    "p.payment_id"
                ).alias(
                    "payments_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col(
                            "p.payment_amount"
                        ),
                        F.lit(0.0),
                    )
                ).alias(
                    "payment_amount_90d"
                ),
            )
        )

        # ====================================================
        # WEBSITE
        # ====================================================

        website = (
            spark.table(
                SILVER_WEBSITE_EVENTS_TABLE
            )
            .select(
                "event_id",
                "customer_id",
                "session_id",
                source_timestamp(
                    "event_timestamp"
                ).alias(
                    "event_timestamp"
                ),
            )
        )

        w = website.alias("w")

        website_features = (
            b.join(
                w,
                (
                    F.col(
                        "b.customer_id"
                    )
                    ==
                    F.col(
                        "w.customer_id"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "w.event_timestamp"
                        )
                    )
                    <=
                    F.col(
                        "b.prediction_date"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "w.event_timestamp"
                        )
                    )
                    >
                    F.date_sub(
                        F.col(
                            "b.prediction_date"
                        ),
                        90,
                    )
                ),
                "left",
            )
            .groupBy(
                "b.customer_id"
            )
            .agg(

                F.countDistinct(
                    F.when(
                        F.to_date(
                            F.col(
                                "w.event_timestamp"
                            )
                        )
                        >
                        F.date_sub(
                            F.col(
                                "b.prediction_date"
                            ),
                            30,
                        ),
                        F.col(
                            "w.event_id"
                        ),
                    )
                ).alias(
                    "website_events_30d"
                ),

                F.countDistinct(
                    "w.event_id"
                ).alias(
                    "website_events_90d"
                ),

                F.countDistinct(
                    F.when(
                        F.to_date(
                            F.col(
                                "w.event_timestamp"
                            )
                        )
                        >
                        F.date_sub(
                            F.col(
                                "b.prediction_date"
                            ),
                            30,
                        ),
                        F.col(
                            "w.session_id"
                        ),
                    )
                ).alias(
                    "sessions_30d"
                ),
            )
        )

        # ====================================================
        # SUPPORT
        # ====================================================

        support = (
            spark.table(
                SILVER_SUPPORT_TICKETS_TABLE
            )
            .select(
                "ticket_id",
                "customer_id",
                source_timestamp(
                    "created_at"
                ).alias(
                    "ticket_created_at"
                ),
            )
        )

        t = support.alias("t")

        support_features = (
            b.join(
                t,
                (
                    F.col(
                        "b.customer_id"
                    )
                    ==
                    F.col(
                        "t.customer_id"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "t.ticket_created_at"
                        )
                    )
                    <=
                    F.col(
                        "b.prediction_date"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "t.ticket_created_at"
                        )
                    )
                    >
                    F.date_sub(
                        F.col(
                            "b.prediction_date"
                        ),
                        90,
                    )
                ),
                "left",
            )
            .groupBy(
                "b.customer_id"
            )
            .agg(
                F.countDistinct(
                    "t.ticket_id"
                ).alias(
                    "support_tickets_90d"
                )
            )
        )

        # ====================================================
        # MARKETING
        # ====================================================

        marketing = (
            spark.table(
                SILVER_MARKETING_EVENTS_TABLE
            )
            .select(
                "marketing_event_id",
                "customer_id",
                source_timestamp(
                    "event_timestamp"
                ).alias(
                    "marketing_timestamp"
                ),
                F.coalesce(
                    F.col(
                        "impression"
                    ).cast("int"),
                    F.lit(0),
                ).alias(
                    "impression"
                ),
                F.coalesce(
                    F.col(
                        "click"
                    ).cast("int"),
                    F.lit(0),
                ).alias(
                    "click"
                ),
                F.coalesce(
                    F.col(
                        "conversion"
                    ).cast("int"),
                    F.lit(0),
                ).alias(
                    "conversion"
                ),
                F.coalesce(
                    F.col(
                        "cost"
                    ).cast("double"),
                    F.lit(0.0),
                ).alias(
                    "marketing_cost"
                ),
            )
        )

        m = marketing.alias("m")

        marketing_features = (
            b.join(
                m,
                (
                    F.col(
                        "b.customer_id"
                    )
                    ==
                    F.col(
                        "m.customer_id"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "m.marketing_timestamp"
                        )
                    )
                    <=
                    F.col(
                        "b.prediction_date"
                    )
                )
                &
                (
                    F.to_date(
                        F.col(
                            "m.marketing_timestamp"
                        )
                    )
                    >
                    F.date_sub(
                        F.col(
                            "b.prediction_date"
                        ),
                        90,
                    )
                ),
                "left",
            )
            .groupBy(
                "b.customer_id"
            )
            .agg(
                F.sum(
                    F.coalesce(
                        F.col(
                            "m.impression"
                        ),
                        F.lit(0),
                    )
                ).alias(
                    "marketing_impressions_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col(
                            "m.click"
                        ),
                        F.lit(0),
                    )
                ).alias(
                    "marketing_clicks_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col(
                            "m.conversion"
                        ),
                        F.lit(0),
                    )
                ).alias(
                    "marketing_conversions_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col(
                            "m.marketing_cost"
                        ),
                        F.lit(0.0),
                    )
                ).alias(
                    "marketing_cost_90d"
                ),
            )
        )

        # ====================================================
        # COMBINE
        # ====================================================

        features = (
            base
            .join(
                order_features.drop(
                    "prediction_date"
                ),
                "customer_id",
                "left",
            )
            .join(
                payment_features,
                "customer_id",
                "left",
            )
            .join(
                website_features,
                "customer_id",
                "left",
            )
            .join(
                support_features,
                "customer_id",
                "left",
            )
            .join(
                marketing_features,
                "customer_id",
                "left",
            )
        )

        # ====================================================
        # NULL HANDLING
        # ====================================================

        zero_columns = [
            "orders_30d",
            "orders_90d",
            "orders_180d",

            "revenue_30d",
            "revenue_90d",
            "revenue_180d",

            "avg_order_value_90d",

            "payments_90d",
            "payment_amount_90d",

            "website_events_30d",
            "website_events_90d",
            "sessions_30d",

            "support_tickets_90d",

            "marketing_impressions_90d",
            "marketing_clicks_90d",
            "marketing_conversions_90d",
            "marketing_cost_90d",
        ]

        for column in zero_columns:

            features = (
                features
                .withColumn(
                    column,
                    F.coalesce(
                        F.col(column),
                        F.lit(0),
                    ),
                )
            )

        features = (
            features
            .withColumn(
                "days_since_last_order",
                F.coalesce(
                    F.col(
                        "days_since_last_order"
                    ),
                    F.lit(9999),
                ),
            )
        )

        # ====================================================
        # TREND FEATURES
        # Must match training.
        # ====================================================

        features = (
            features
            .withColumn(
                "order_frequency_ratio",
                F.when(
                    F.col(
                        "orders_90d"
                    ) > 0,
                    F.col(
                        "orders_30d"
                    )
                    /
                    (
                        F.col(
                            "orders_90d"
                        )
                        / 3.0
                    ),
                ).otherwise(
                    F.lit(0.0)
                ),
            )
            .withColumn(
                "revenue_frequency_ratio",
                F.when(
                    F.col(
                        "revenue_90d"
                    ) > 0,
                    F.col(
                        "revenue_30d"
                    )
                    /
                    (
                        F.col(
                            "revenue_90d"
                        )
                        / 3.0
                    ),
                ).otherwise(
                    F.lit(0.0)
                ),
            )
        )

        # ====================================================
        # FINAL OUTPUT
        # ====================================================

        features = features.select(
            "customer_id",
            "prediction_date",

            "customer_segment",
            "country",
            "state",

            "tenure_days",
            "days_since_last_order",

            "orders_30d",
            "orders_90d",
            "orders_180d",

            "revenue_30d",
            "revenue_90d",
            "revenue_180d",

            "avg_order_value_90d",

            "payments_90d",
            "payment_amount_90d",

            "website_events_30d",
            "website_events_90d",
            "sessions_30d",

            "support_tickets_90d",

            "marketing_impressions_90d",
            "marketing_clicks_90d",
            "marketing_conversions_90d",
            "marketing_cost_90d",

            "order_frequency_ratio",
            "revenue_frequency_ratio",
        )

        print()
        print("=" * 70)
        print("CURRENT CHURN SCORING FEATURES")
        print("=" * 70)

        features.printSchema()

        features.show(
            20,
            truncate=False,
        )

        count = features.count()

        print(
            "Customers:",
            count,
        )

        spark.sql(
            """
            CREATE NAMESPACE IF NOT EXISTS
            retailpulse.ml
            """
        )

        (
            features
            .writeTo(
                CHURN_SCORING_FEATURES_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "SUCCESS:",
            CHURN_SCORING_FEATURES_TABLE,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
