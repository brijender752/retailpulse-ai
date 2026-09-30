from __future__ import annotations

import os
import sys

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)

from iceberg_session import create_iceberg_spark_session


CUSTOMER_360_TABLE = (
    "retailpulse.gold.customer_360"
)

CHURN_TABLE = (
    "retailpulse.ml.churn_predictions"
)

RECOMMENDATION_TABLE = (
    "retailpulse.ml.product_recommendations"
)


POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "postgres",
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432",
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "retailpulse",
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "postgres",
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "postgres",
)


JDBC_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{POSTGRES_DB}"
)


JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def require_table(
    spark,
    table_name,
):

    if not spark.catalog.tableExists(
        table_name
    ):

        raise RuntimeError(
            f"Required table does not exist: "
            f"{table_name}"
        )


def select_available(
    df,
    columns,
):

    available = [
        column
        for column in columns
        if column in df.columns
    ]

    return df.select(
        *available
    )


def publish_customer_360(
    spark,
):

    print()
    print("Publishing Customer 360...")

    df = spark.table(
        CUSTOMER_360_TABLE
    )

    # --------------------------------------------------------
    # Adjust aliases here if your Customer360 table uses
    # slightly different names.
    # --------------------------------------------------------

    required = {
        "customer_id",
    }

    missing = (
        required
        -
        set(df.columns)
    )

    if missing:

        raise RuntimeError(
            "Customer360 missing: "
            + ", ".join(
                sorted(missing)
            )
        )

    wanted = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "state",
        "city",
        "customer_segment",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "days_since_last_order",
        "total_payments",
        "total_website_events",
        "total_support_tickets",
    ]

    df = select_available(
        df,
        wanted,
    )

    # PostgreSQL table expects every field.
    optional_defaults = {
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "country": "string",
        "state": "string",
        "city": "string",
        "customer_segment": "string",
        "total_orders": "long",
        "total_revenue": "decimal(18,2)",
        "avg_order_value": "decimal(18,2)",
        "days_since_last_order": "long",
        "total_payments": "long",
        "total_website_events": "long",
        "total_support_tickets": "long",
    }

    for column, data_type in (
        optional_defaults.items()
    ):

        if column not in df.columns:

            df = df.withColumn(
                column,
                F.lit(None).cast(
                    data_type
                ),
            )

    df = (
        df
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "state",
            "city",
            "customer_segment",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "days_since_last_order",
            "total_payments",
            "total_website_events",
            "total_support_tickets",
        )
        .dropDuplicates(
            ["customer_id"]
        )
    )

    count = df.count()

    print(
        "Customer360 rows:",
        f"{count:,}",
    )

    if count == 0:

        raise RuntimeError(
            "Customer360 is empty."
        )

    (
        df.write
        .jdbc(
            url=JDBC_URL,
            table=(
                "serving.customer_360"
            ),
            mode="overwrite",
            properties=JDBC_PROPERTIES,
        )
    )


def publish_churn(
    spark,
):

    print()
    print("Publishing churn predictions...")

    df = spark.table(
        CHURN_TABLE
    )

    required = {
        "customer_id",
        "churn_probability",
    }

    missing = (
        required
        -
        set(df.columns)
    )

    if missing:

        raise RuntimeError(
            "Churn predictions missing: "
            + ", ".join(
                sorted(missing)
            )
        )

    # --------------------------------------------------------
    # Keep latest prediction per customer if historical
    # prediction rows exist.
    # --------------------------------------------------------

    if "prediction_date" not in df.columns:

        df = df.withColumn(
            "prediction_date",
            F.current_timestamp(),
        )

    if "churn_prediction" not in df.columns:

        df = df.withColumn(
            "churn_prediction",

            F.when(
                F.col(
                    "churn_probability"
                ) >= 0.5,
                F.lit(1),
            ).otherwise(
                F.lit(0)
            ),
        )

    if "risk_level" not in df.columns:

        df = df.withColumn(
            "risk_level",

            F.when(
                F.col(
                    "churn_probability"
                ) >= 0.80,
                F.lit("CRITICAL"),
            )
            .when(
                F.col(
                    "churn_probability"
                ) >= 0.60,
                F.lit("HIGH"),
            )
            .when(
                F.col(
                    "churn_probability"
                ) >= 0.30,
                F.lit("MEDIUM"),
            )
            .otherwise(
                F.lit("LOW")
            ),
        )

    if "model_version" not in df.columns:

        df = df.withColumn(
            "model_version",
            F.lit("unknown"),
        )

    from pyspark.sql import Window

    window = (
        Window
        .partitionBy(
            "customer_id"
        )
        .orderBy(
            F.desc(
                "prediction_date"
            )
        )
    )

    df = (
        df
        .withColumn(
            "_rn",
            F.row_number().over(
                window
            ),
        )
        .filter(
            F.col("_rn") == 1
        )
        .drop("_rn")
        .select(
            "customer_id",
            "churn_probability",
            "churn_prediction",
            "risk_level",
            "model_version",
            "prediction_date",
        )
    )

    count = df.count()

    print(
        "Churn rows:",
        f"{count:,}",
    )

    if count == 0:

        raise RuntimeError(
            "Churn predictions are empty."
        )

    (
        df.write
        .jdbc(
            url=JDBC_URL,
            table=(
                "serving.churn_predictions"
            ),
            mode="overwrite",
            properties=JDBC_PROPERTIES,
        )
    )


def publish_recommendations(
    spark,
):

    print()
    print(
        "Publishing recommendations..."
    )

    df = spark.table(
        RECOMMENDATION_TABLE
    )

    required = {
        "customer_id",
        "product_id",
        "recommendation_rank",
    }

    missing = (
        required
        -
        set(df.columns)
    )

    if missing:

        raise RuntimeError(
            "Recommendations missing: "
            + ", ".join(
                sorted(missing)
            )
        )

    defaults = {
        "recommendation_score": "double",
        "recommendation_reason": "string",
        "model_name": "string",
        "model_version": "string",
        "generated_at": "timestamp",
    }

    for column, data_type in (
        defaults.items()
    ):

        if column not in df.columns:

            value = (
                F.current_timestamp()
                if column == "generated_at"
                else
                F.lit(None).cast(
                    data_type
                )
            )

            df = df.withColumn(
                column,
                value,
            )

    df = (
        df
        .select(
            "customer_id",
            "product_id",
            "recommendation_rank",
            "recommendation_score",
            "recommendation_reason",
            "model_name",
            "model_version",
            "generated_at",
        )
        .dropDuplicates(
            [
                "customer_id",
                "product_id",
            ]
        )
    )

    count = df.count()

    print(
        "Recommendation rows:",
        f"{count:,}",
    )

    if count == 0:

        raise RuntimeError(
            "Recommendations are empty."
        )

    (
        df.write
        .jdbc(
            url=JDBC_URL,
            table=(
                "serving.product_recommendations"
            ),
            mode="overwrite",
            properties=JDBC_PROPERTIES,
        )
    )


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse - Publish API Serving Tables"
    )

    try:

        print()
        print("=" * 70)
        print("PUBLISH API SERVING TABLES")
        print("=" * 70)

        require_table(
            spark,
            CUSTOMER_360_TABLE,
        )

        require_table(
            spark,
            CHURN_TABLE,
        )

        require_table(
            spark,
            RECOMMENDATION_TABLE,
        )

        publish_customer_360(
            spark
        )

        publish_churn(
            spark
        )

        publish_recommendations(
            spark
        )

        print()
        print("=" * 70)
        print("SERVING PUBLISH COMPLETE")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()