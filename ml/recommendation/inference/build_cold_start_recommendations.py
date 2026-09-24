from __future__ import annotations

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import (
    create_iceberg_spark_session,
)


CUSTOMERS_TABLE = (
    "retailpulse.analytics.dim_customer"
)

INTERACTIONS_TABLE = (
    "retailpulse.ml.customer_product_interactions"
)

POPULARITY_TABLE = (
    "retailpulse.ml.product_popularity"
)

OUTPUT_TABLE = (
    "retailpulse.ml.cold_start_recommendations"
)

TOP_N = 10


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Cold Start Recommendations"
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
                "customer_id"
            )
            .distinct()
        )

        # ====================================================
        # CUSTOMERS WITH HISTORY
        # ====================================================

        active_customers = (
            spark.table(
                INTERACTIONS_TABLE
            )
            .select(
                "customer_id"
            )
            .distinct()
        )

        # ====================================================
        # COLD START CUSTOMERS
        # ====================================================

        cold_customers = (
            customers
            .join(
                active_customers,
                "customer_id",
                "left_anti",
            )
        )

        cold_count = (
            cold_customers.count()
        )

        print(
            "Cold-start customers:",
            cold_count,
        )

        if cold_count == 0:

            print(
                "No cold-start customers found."
            )

            return

        # ====================================================
        # TOP GLOBAL PRODUCTS
        # ====================================================

        popular_products = (
            spark.table(
                POPULARITY_TABLE
            )
            .filter(
                F.col(
                    "global_rank"
                )
                <=
                TOP_N
            )
            .select(
                "product_id",
                "product_name",
                "category",
                "brand",
                "price",
                "popularity_score",
                "global_rank",
            )
        )

        # ====================================================
        # CUSTOMER × TOP PRODUCTS
        # ====================================================

        recommendations = (
            cold_customers
            .crossJoin(
                popular_products
            )
            .withColumn(
                "recommendation_rank",
                F.col(
                    "global_rank"
                ),
            )
            .withColumn(
                "recommendation_score",
                F.col(
                    "popularity_score"
                ),
            )
            .withColumn(
                "recommendation_reason",
                F.lit(
                    "popular_product"
                ),
            )
            .withColumn(
                "model_name",
                F.lit(
                    "popularity_baseline_v1"
                ),
            )
            .withColumn(
                "generated_at",
                F.current_timestamp(),
            )
        )

        recommendations = (
            recommendations
            .select(
                "customer_id",
                "product_id",
                "product_name",
                "category",
                "brand",
                "price",

                "recommendation_score",
                "recommendation_rank",
                "recommendation_reason",

                "model_name",
                "generated_at",
            )
        )

        print()
        print(
            "Recommendation rows:",
            recommendations.count(),
        )

        recommendations.show(
            30,
            truncate=False,
        )

        (
            recommendations
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