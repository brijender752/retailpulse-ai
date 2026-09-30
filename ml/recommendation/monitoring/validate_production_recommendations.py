from __future__ import annotations

import sys

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)

from iceberg_session import create_iceberg_spark_session


RECOMMENDATIONS_TABLE = (
    "retailpulse.ml.product_recommendations"
)

INTERACTIONS_TABLE = (
    "retailpulse.ml.customer_product_interactions"
)

CUSTOMERS_TABLE = (
    "retailpulse.analytics.dim_customer"
)

TOP_K = 10


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse - Validate Recommendations"
    )

    try:

        print("=" * 70)
        print("VALIDATE PRODUCTION RECOMMENDATIONS")
        print("=" * 70)

        if not spark.catalog.tableExists(
            RECOMMENDATIONS_TABLE
        ):

            raise RuntimeError(
                "Recommendation serving table does not exist."
            )

        recommendations = spark.table(
            RECOMMENDATIONS_TABLE
        )

        count = recommendations.count()

        print()
        print(
            "Recommendations:",
            f"{count:,}",
        )

        if count == 0:

            raise RuntimeError(
                "Recommendation table is empty."
            )

        required_columns = {
            "customer_id",
            "product_id",
            "recommendation_rank",
            "recommendation_score",
            "recommendation_reason",
            "run_id",
            "generated_at",
        }

        missing = (
            required_columns
            -
            set(recommendations.columns)
        )

        if missing:

            raise RuntimeError(
                "Recommendation table missing columns: "
                + ", ".join(sorted(missing))
            )

        # ----------------------------------------------------
        # NULL CHECK
        # ----------------------------------------------------

        invalid_nulls = (
            recommendations
            .filter(
                F.col("customer_id").isNull()
                |
                F.col("product_id").isNull()
                |
                F.col("recommendation_rank").isNull()
                |
                F.col("recommendation_score").isNull()
                |
                F.col("run_id").isNull()
            )
            .count()
        )

        if invalid_nulls:

            raise RuntimeError(
                f"Found {invalid_nulls} recommendations "
                "with required NULL values."
            )

        # ----------------------------------------------------
        # DUPLICATES
        # ----------------------------------------------------

        duplicates = (
            recommendations
            .groupBy(
                "customer_id",
                "product_id",
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        if duplicates:

            raise RuntimeError(
                f"Found {duplicates} duplicate "
                "customer-product recommendations."
            )

        # ----------------------------------------------------
        # TOP K
        # ----------------------------------------------------

        over_limit = (
            recommendations
            .groupBy("customer_id")
            .count()
            .filter(
                F.col("count") > TOP_K
            )
            .count()
        )

        if over_limit:

            raise RuntimeError(
                f"{over_limit} customers have more "
                f"than {TOP_K} recommendations."
            )

        # ----------------------------------------------------
        # RANK VALIDATION
        # ----------------------------------------------------

        invalid_ranks = (
            recommendations
            .filter(
                (F.col("recommendation_rank") < 1)
                |
                (F.col("recommendation_rank") > TOP_K)
            )
            .count()
        )

        if invalid_ranks:

            raise RuntimeError(
                f"Found {invalid_ranks} invalid ranks."
            )

        # ----------------------------------------------------
        # SCORE VALIDATION
        # ----------------------------------------------------

        invalid_scores = (
            recommendations
            .filter(
                F.isnan("recommendation_score")
                |
                F.col("recommendation_score").isNull()
                |
                (F.col("recommendation_score") < 0)
            )
            .count()
        )

        if invalid_scores:

            raise RuntimeError(
                f"Found {invalid_scores} invalid scores."
            )

        # ----------------------------------------------------
        # SEEN PRODUCT LEAKAGE
        # ----------------------------------------------------

        interactions = (
            spark.table(
                INTERACTIONS_TABLE
            )
            .select(
                "customer_id",
                "product_id",
            )
            .distinct()
        )

        seen_recommendations = (
            recommendations
            .select(
                "customer_id",
                "product_id",
            )
            .join(
                interactions,
                [
                    "customer_id",
                    "product_id",
                ],
                "inner",
            )
            .count()
        )

        if seen_recommendations:

            raise RuntimeError(
                f"{seen_recommendations} recommendations "
                "were already seen by the customer."
            )

        # ----------------------------------------------------
        # RUN CONSISTENCY
        # ----------------------------------------------------

        run_count = (
            recommendations
            .select("run_id")
            .distinct()
            .count()
        )

        if run_count != 1:

            raise RuntimeError(
                "Current serving snapshot should contain "
                "exactly one run_id. "
                f"Found {run_count}."
            )

        # ----------------------------------------------------
        # COVERAGE
        # ----------------------------------------------------

        total_customers = (
            spark.table(
                CUSTOMERS_TABLE
            )
            .select("customer_id")
            .distinct()
            .count()
        )

        recommended_customers = (
            recommendations
            .select("customer_id")
            .distinct()
            .count()
        )

        coverage = (
            recommended_customers
            /
            total_customers
            if total_customers
            else 0
        )

        print()
        print(
            "Recommended customers:",
            f"{recommended_customers:,}",
        )

        print(
            "Total customers:",
            f"{total_customers:,}",
        )

        print(
            "Customer coverage:",
            f"{coverage:.4f}",
        )

        print()
        print("=" * 70)
        print("RECOMMENDATION VALIDATION SUCCESS")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()