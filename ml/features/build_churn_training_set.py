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

from common.ml_config import (
    CUSTOMERS_TABLE,
    CHURN_TRAINING_TABLE,
    SILVER_ORDERS_TABLE,
    SILVER_PAYMENTS_TABLE,
    SILVER_WEBSITE_EVENTS_TABLE,
    SILVER_SUPPORT_TICKETS_TABLE,
    SILVER_MARKETING_EVENTS_TABLE,
    CHURN_HORIZON_DAYS,
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Historical Churn Training"
    )

    try:

        # -----------------------------------------------------
        # 1. Load source tables
        # -----------------------------------------------------

        customers = (
            spark.table(CUSTOMERS_TABLE)
            .select(
                "customer_id",
                source_timestamp("signup_date").alias("signup_date"),
                "customer_segment",
                "country",
                "state",
            )
        )

        orders = (
            spark.table(SILVER_ORDERS_TABLE)
            .select(
                "order_id",
                "customer_id",
                source_timestamp("order_date").alias("order_date"),
                "status",
                F.col("total_amount").cast("double").alias("total_amount"),
            )
        )

        # Remove cancelled orders
        orders = orders.filter(
            ~F.upper(
                F.coalesce(
                    F.col("status"),
                    F.lit("")
                )
            ).isin(
                "CANCELLED",
                "CANCELED"
            )
        )

        payments = (
            spark.table(SILVER_PAYMENTS_TABLE)
            .select(
                "payment_id",
                "customer_id",
                source_timestamp(
                    "transaction_timestamp"
                ).alias("payment_timestamp"),
                F.col("amount").cast("double").alias("payment_amount"),
            )
        )

        website = (
            spark.table(SILVER_WEBSITE_EVENTS_TABLE)
            .select(
                "event_id",
                "customer_id",
                "session_id",
                source_timestamp(
                    "event_timestamp"
                ).alias("event_timestamp"),
            )
        )

        support = (
            spark.table(SILVER_SUPPORT_TICKETS_TABLE)
            .select(
                "ticket_id",
                "customer_id",
                source_timestamp(
                    "created_at"
                ).alias("ticket_created_at"),
            )
        )

        marketing = (
            spark.table(SILVER_MARKETING_EVENTS_TABLE)
            .select(
                "marketing_event_id",
                "customer_id",
                source_timestamp(
                    "event_timestamp"
                ).alias("marketing_timestamp"),
                F.coalesce(
                    F.col("impression").cast("int"),
                    F.lit(0)
                ).alias("impression"),
                F.coalesce(
                    F.col("click").cast("int"),
                    F.lit(0)
                ).alias("click"),
                F.coalesce(
                    F.col("conversion").cast("int"),
                    F.lit(0)
                ).alias("conversion"),
                F.coalesce(
                    F.col("cost").cast("double"),
                    F.lit(0.0)
                ).alias("marketing_cost"),
            )
        )

        # -----------------------------------------------------
        # 2. Determine safe historical range
        # -----------------------------------------------------

        max_order_date = (
            orders
            .agg(
                F.max("order_date").alias("max_date")
            )
            .first()["max_date"]
        )

        min_order_date = (
            orders
            .agg(
                F.min("order_date").alias("min_date")
            )
            .first()["min_date"]
        )

        if max_order_date is None:
            raise RuntimeError(
                "No order data found."
            )

        print()
        print("Order history:")
        print("Min:", min_order_date)
        print("Max:", max_order_date)

        # Observation date must leave 60 days
        # of FUTURE data for the churn label.

        last_observation_date = (
            max_order_date.date()
        )

        # We'll subtract horizon using Spark below.

        # -----------------------------------------------------
        # 3. Generate monthly observation dates
        # -----------------------------------------------------

        date_bounds = spark.createDataFrame(
            [
                (
                    min_order_date.date(),
                    max_order_date.date(),
                )
            ],
            [
                "min_date",
                "max_date",
            ],
        )

        observation_dates = (
            date_bounds
            .select(
                F.explode(
                    F.sequence(
                        F.add_months(
                            F.trunc(
                                F.col("min_date"),
                                "month"
                            ),
                            6
                        ),
                        F.date_sub(
                            F.col("max_date"),
                            CHURN_HORIZON_DAYS
                        ),
                        F.expr("INTERVAL 1 MONTH")
                    )
                ).alias("observation_date")
            )
        )

        print()
        print("Observation dates:")

        observation_dates.show(
            100,
            truncate=False
        )

        observation_count = (
            observation_dates.count()
        )

        if observation_count < 2:
            raise RuntimeError(
                "Not enough historical data to create "
                "multiple churn observation dates. "
                "RetailPulse needs a longer synthetic "
                "history before model training."
            )

        # -----------------------------------------------------
        # 4. Customer × observation-date snapshots
        # -----------------------------------------------------

        snapshots = (
            customers
            .crossJoin(observation_dates)
            .filter(
                F.to_date("signup_date")
                <=
                F.col("observation_date")
            )
        )

        snapshots = (
            snapshots
            .withColumn(
                "snapshot_id",
                F.concat_ws(
                    "_",
                    F.col("customer_id"),
                    F.date_format(
                        "observation_date",
                        "yyyyMMdd"
                    )
                )
            )
            .withColumn(
                "tenure_days",
                F.datediff(
                    F.col("observation_date"),
                    F.to_date("signup_date")
                )
            )
        )

        # -----------------------------------------------------
        # 5. Historical ORDER features
        # -----------------------------------------------------

        s = snapshots.alias("s")
        o = orders.alias("o")

        order_history = (
            s.join(
                o,
                (
                    F.col("s.customer_id")
                    ==
                    F.col("o.customer_id")
                )
                &
                (
                    F.to_date(
                        F.col("o.order_date")
                    )
                    <=
                    F.col("s.observation_date")
                )
                &
                (
                    F.to_date(
                        F.col("o.order_date")
                    )
                    >
                    F.date_sub(
                        F.col("s.observation_date"),
                        180
                    )
                ),
                "left"
            )
            .groupBy(
                "s.snapshot_id",
                "s.customer_id",
                "s.observation_date",
            )
            .agg(

                # -------------------------
                # Recency
                # -------------------------

                F.max(
                    F.to_date("o.order_date")
                ).alias("last_order_date"),

                # -------------------------
                # 30-day orders
                # -------------------------

                F.countDistinct(
                    F.when(
                        F.to_date("o.order_date")
                        >
                        F.date_sub(
                            F.col(
                                "s.observation_date"
                            ),
                            30
                        ),
                        F.col("o.order_id")
                    )
                ).alias("orders_30d"),

                # -------------------------
                # 90-day orders
                # -------------------------

                F.countDistinct(
                    F.when(
                        F.to_date("o.order_date")
                        >
                        F.date_sub(
                            F.col(
                                "s.observation_date"
                            ),
                            90
                        ),
                        F.col("o.order_id")
                    )
                ).alias("orders_90d"),

                # -------------------------
                # 180-day orders
                # -------------------------

                F.countDistinct(
                    "o.order_id"
                ).alias("orders_180d"),

                # -------------------------
                # Revenue 30d
                # -------------------------

                F.sum(
                    F.when(
                        F.to_date("o.order_date")
                        >
                        F.date_sub(
                            F.col(
                                "s.observation_date"
                            ),
                            30
                        ),
                        F.col("o.total_amount")
                    ).otherwise(0.0)
                ).alias("revenue_30d"),

                # -------------------------
                # Revenue 90d
                # -------------------------

                F.sum(
                    F.when(
                        F.to_date("o.order_date")
                        >
                        F.date_sub(
                            F.col(
                                "s.observation_date"
                            ),
                            90
                        ),
                        F.col("o.total_amount")
                    ).otherwise(0.0)
                ).alias("revenue_90d"),

                # -------------------------
                # Revenue 180d
                # -------------------------

                F.sum(
                    F.coalesce(
                        F.col("o.total_amount"),
                        F.lit(0.0)
                    )
                ).alias("revenue_180d"),
            )
        )

        order_history = (
            order_history
            .withColumn(
                "days_since_last_order",
                F.when(
                    F.col(
                        "last_order_date"
                    ).isNotNull(),
                    F.datediff(
                        F.col(
                            "observation_date"
                        ),
                        F.col(
                            "last_order_date"
                        )
                    )
                ).otherwise(
                    F.lit(9999)
                )
            )
            .withColumn(
                "avg_order_value_90d",
                F.when(
                    F.col("orders_90d") > 0,
                    F.col("revenue_90d")
                    /
                    F.col("orders_90d")
                ).otherwise(0.0)
            )
        )

        # -----------------------------------------------------
        # 6. Payment features
        # -----------------------------------------------------

        p = payments.alias("p")

        payment_features = (
            s.join(
                p,
                (
                    F.col("s.customer_id")
                    ==
                    F.col("p.customer_id")
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
                        "s.observation_date"
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
                            "s.observation_date"
                        ),
                        90
                    )
                ),
                "left"
            )
            .groupBy(
                "s.snapshot_id"
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
                        F.lit(0.0)
                    )
                ).alias(
                    "payment_amount_90d"
                ),
            )
        )

        # -----------------------------------------------------
        # 7. Website features
        # -----------------------------------------------------

        w = website.alias("w")

        website_features = (
            s.join(
                w,
                (
                    F.col("s.customer_id")
                    ==
                    F.col("w.customer_id")
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
                        "s.observation_date"
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
                            "s.observation_date"
                        ),
                        90
                    )
                ),
                "left"
            )
            .groupBy(
                "s.snapshot_id"
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
                                "s.observation_date"
                            ),
                            30
                        ),
                        F.col(
                            "w.event_id"
                        )
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
                                "s.observation_date"
                            ),
                            30
                        ),
                        F.col(
                            "w.session_id"
                        )
                    )
                ).alias(
                    "sessions_30d"
                ),
            )
        )

        # -----------------------------------------------------
        # 8. Support features
        # -----------------------------------------------------

        t = support.alias("t")

        support_features = (
            s.join(
                t,
                (
                    F.col("s.customer_id")
                    ==
                    F.col("t.customer_id")
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
                        "s.observation_date"
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
                            "s.observation_date"
                        ),
                        90
                    )
                ),
                "left"
            )
            .groupBy(
                "s.snapshot_id"
            )
            .agg(
                F.countDistinct(
                    "t.ticket_id"
                ).alias(
                    "support_tickets_90d"
                )
            )
        )

        # -----------------------------------------------------
        # 9. Marketing features
        # -----------------------------------------------------

        m = marketing.alias("m")

        marketing_features = (
            s.join(
                m,
                (
                    F.col("s.customer_id")
                    ==
                    F.col("m.customer_id")
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
                        "s.observation_date"
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
                            "s.observation_date"
                        ),
                        90
                    )
                ),
                "left"
            )
            .groupBy(
                "s.snapshot_id"
            )
            .agg(
                F.sum(
                    F.coalesce(
                        F.col("m.impression"),
                        F.lit(0)
                    )
                ).alias(
                    "marketing_impressions_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col("m.click"),
                        F.lit(0)
                    )
                ).alias(
                    "marketing_clicks_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col("m.conversion"),
                        F.lit(0)
                    )
                ).alias(
                    "marketing_conversions_90d"
                ),

                F.sum(
                    F.coalesce(
                        F.col(
                            "m.marketing_cost"
                        ),
                        F.lit(0.0)
                    )
                ).alias(
                    "marketing_cost_90d"
                ),
            )
        )

        # -----------------------------------------------------
        # 10. Create future churn label
        # -----------------------------------------------------

        future_orders = (
            s.join(
                o,
                (
                    F.col("s.customer_id")
                    ==
                    F.col("o.customer_id")
                )
                &
                (
                    F.to_date(
                        F.col("o.order_date")
                    )
                    >
                    F.col(
                        "s.observation_date"
                    )
                )
                &
                (
                    F.to_date(
                        F.col("o.order_date")
                    )
                    <=
                    F.date_add(
                        F.col(
                            "s.observation_date"
                        ),
                        CHURN_HORIZON_DAYS
                    )
                ),
                "left"
            )
            .groupBy(
                "s.snapshot_id"
            )
            .agg(
                F.countDistinct(
                    "o.order_id"
                ).alias(
                    "future_orders_60d"
                )
            )
            .withColumn(
                "churned",
                F.when(
                    F.col(
                        "future_orders_60d"
                    ) == 0,
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )
            )
        )

        # -----------------------------------------------------
        # 11. Assemble training dataset
        # -----------------------------------------------------

        training = (
            snapshots

            .join(
                order_history.drop(
                    "customer_id",
                    "observation_date"
                ),
                "snapshot_id",
                "left"
            )

            .join(
                payment_features,
                "snapshot_id",
                "left"
            )

            .join(
                website_features,
                "snapshot_id",
                "left"
            )

            .join(
                support_features,
                "snapshot_id",
                "left"
            )

            .join(
                marketing_features,
                "snapshot_id",
                "left"
            )

            .join(
                future_orders,
                "snapshot_id",
                "left"
            )
        )

        # -----------------------------------------------------
        # 12. Null handling
        # -----------------------------------------------------

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

            "future_orders_60d",
        ]

        for column in zero_columns:

            training = (
                training.withColumn(
                    column,
                    F.coalesce(
                        F.col(column),
                        F.lit(0)
                    )
                )
            )

        training = (
            training
            .withColumn(
                "days_since_last_order",
                F.coalesce(
                    F.col(
                        "days_since_last_order"
                    ),
                    F.lit(9999)
                )
            )
        )

        # -----------------------------------------------------
        # 13. Behavior trend features
        # -----------------------------------------------------

        training = (
            training

            .withColumn(
                "order_frequency_ratio",
                F.when(
                    F.col("orders_90d") > 0,
                    F.col("orders_30d")
                    /
                    (
                        F.col("orders_90d")
                        / 3.0
                    )
                ).otherwise(0.0)
            )

            .withColumn(
                "revenue_frequency_ratio",
                F.when(
                    F.col("revenue_90d") > 0,
                    F.col("revenue_30d")
                    /
                    (
                        F.col("revenue_90d")
                        / 3.0
                    )
                ).otherwise(0.0)
            )
        )

        # -----------------------------------------------------
        # 14. Select final columns
        # -----------------------------------------------------

        training = training.select(

            "snapshot_id",
            "customer_id",
            "observation_date",

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

            "future_orders_60d",

            "churned",
        )

        # -----------------------------------------------------
        # 15. Validation
        # -----------------------------------------------------

        print()
        print("=" * 70)
        print("CHURN TRAINING DATASET")
        print("=" * 70)

        training.printSchema()

        training.show(
            20,
            truncate=False
        )

        print()
        print("Training rows:", training.count())

        print()
        print("Churn distribution:")

        (
            training
            .groupBy("churned")
            .count()
            .orderBy("churned")
            .show()
        )

        # -----------------------------------------------------
        # 16. Write Iceberg table
        # -----------------------------------------------------

        spark.sql(
            """
            CREATE NAMESPACE IF NOT EXISTS
            retailpulse.ml
            """
        )

        (
            training
            .writeTo(
                CHURN_TRAINING_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "SUCCESS:",
            CHURN_TRAINING_TABLE,
            "created."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
