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

CUSTOMERS_TABLE = (
    "retailpulse.analytics.dim_customer"
)

PRODUCTS_TABLE = (
    "retailpulse.silver.products"
)

MONITORING_TABLE = (
    "retailpulse.ml.recommendation_monitoring"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse - Recommendation Monitoring"
    )

    try:

        print("=" * 70)
        print("RECOMMENDATION MONITORING")
        print("=" * 70)

        recommendations = (
            spark.table(
                RECOMMENDATIONS_TABLE
            )
        )

        if recommendations.limit(1).count() == 0:

            raise RuntimeError(
                "Recommendation table is empty."
            )

        # ----------------------------------------------------
        # RUN METADATA
        # ----------------------------------------------------

        run_rows = (
            recommendations
            .select(
                "run_id",
                "model_name",
                "model_version",
                "personalized_weight",
                "popularity_weight",
            )
            .distinct()
            .collect()
        )

        if len(run_rows) != 1:

            raise RuntimeError(
                "Expected exactly one recommendation "
                "run in serving snapshot."
            )

        run = run_rows[0]

        run_id = run["run_id"]

        # ----------------------------------------------------
        # BASIC COUNTS
        # ----------------------------------------------------

        total_recommendations = (
            recommendations.count()
        )

        recommended_customers = (
            recommendations
            .select("customer_id")
            .distinct()
            .count()
        )

        recommended_products = (
            recommendations
            .select("product_id")
            .distinct()
            .count()
        )

        total_customers = (
            spark.table(
                CUSTOMERS_TABLE
            )
            .select("customer_id")
            .distinct()
            .count()
        )

        total_products = (
            spark.table(
                PRODUCTS_TABLE
            )
            .select("product_id")
            .distinct()
            .count()
        )

        customer_coverage = (
            recommended_customers
            /
            total_customers
            if total_customers
            else 0.0
        )

        catalog_coverage = (
            recommended_products
            /
            total_products
            if total_products
            else 0.0
        )

        avg_recommendations = (
            total_recommendations
            /
            recommended_customers
            if recommended_customers
            else 0.0
        )

        # ----------------------------------------------------
        # SCORE STATISTICS
        # ----------------------------------------------------

        score_stats = (
            recommendations
            .agg(
                F.avg(
                    "recommendation_score"
                ).alias(
                    "avg_score"
                ),

                F.min(
                    "recommendation_score"
                ).alias(
                    "min_score"
                ),

                F.max(
                    "recommendation_score"
                ).alias(
                    "max_score"
                ),

                F.expr(
                    "percentile_approx("
                    "recommendation_score, 0.5)"
                ).alias(
                    "median_score"
                ),
            )
            .first()
        )

        # ----------------------------------------------------
        # RECOMMENDATION REASONS
        # ----------------------------------------------------

        reason_rows = (
            recommendations
            .groupBy(
                "recommendation_reason"
            )
            .count()
            .collect()
        )

        reason_counts = {
            row[
                "recommendation_reason"
            ]: int(
                row["count"]
            )
            for row in reason_rows
        }

        personalized_count = (
            reason_counts.get(
                "hybrid_personalized",
                0,
            )
            +
            reason_counts.get(
                "personalized_similarity",
                0,
            )
        )

        fallback_count = (
            reason_counts.get(
                "popular_fallback",
                0,
            )
        )

        cold_start_count = (
            reason_counts.get(
                "popular_cold_start",
                0,
            )
        )

        personalized_pct = (
            personalized_count
            /
            total_recommendations
            if total_recommendations
            else 0.0
        )

        fallback_pct = (
            fallback_count
            /
            total_recommendations
            if total_recommendations
            else 0.0
        )

        cold_start_pct = (
            cold_start_count
            /
            total_recommendations
            if total_recommendations
            else 0.0
        )

        # ----------------------------------------------------
        # QUALITY CHECK METRICS
        # ----------------------------------------------------

        duplicate_pairs = (
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

        invalid_scores = (
            recommendations
            .filter(
                F.col(
                    "recommendation_score"
                ).isNull()
                |
                F.isnan(
                    "recommendation_score"
                )
                |
                (
                    F.col(
                        "recommendation_score"
                    ) < 0
                )
            )
            .count()
        )

        # ----------------------------------------------------
        # CREATE MONITORING ROW
        # ----------------------------------------------------

        monitoring = (
            spark.createDataFrame(
                [
                    (
                        str(run_id),
                        str(run["model_name"]),
                        str(run["model_version"]),
                        float(
                            run[
                                "personalized_weight"
                            ]
                        ),
                        float(
                            run[
                                "popularity_weight"
                            ]
                        ),
                        int(
                            total_customers
                        ),
                        int(
                            recommended_customers
                        ),
                        float(
                            customer_coverage
                        ),
                        int(
                            total_products
                        ),
                        int(
                            recommended_products
                        ),
                        float(
                            catalog_coverage
                        ),
                        int(
                            total_recommendations
                        ),
                        float(
                            avg_recommendations
                        ),
                        float(
                            score_stats[
                                "avg_score"
                            ] or 0
                        ),
                        float(
                            score_stats[
                                "median_score"
                            ] or 0
                        ),
                        float(
                            score_stats[
                                "min_score"
                            ] or 0
                        ),
                        float(
                            score_stats[
                                "max_score"
                            ] or 0
                        ),
                        int(
                            personalized_count
                        ),
                        float(
                            personalized_pct
                        ),
                        int(
                            fallback_count
                        ),
                        float(
                            fallback_pct
                        ),
                        int(
                            cold_start_count
                        ),
                        float(
                            cold_start_pct
                        ),
                        int(
                            duplicate_pairs
                        ),
                        int(
                            invalid_scores
                        ),
                    )
                ],
                """
                run_id string,
                model_name string,
                model_version string,
                personalized_weight double,
                popularity_weight double,
                total_customers long,
                recommended_customers long,
                customer_coverage double,
                total_products long,
                recommended_products long,
                catalog_coverage double,
                total_recommendations long,
                avg_recommendations_per_customer double,
                avg_score double,
                median_score double,
                min_score double,
                max_score double,
                personalized_count long,
                personalized_pct double,
                fallback_count long,
                fallback_pct double,
                cold_start_count long,
                cold_start_pct double,
                duplicate_pairs long,
                invalid_scores long
                """,
            )
            .withColumn(
                "monitored_at",
                F.current_timestamp(),
            )
        )

        monitoring.show(
            truncate=False
        )

        # ----------------------------------------------------
        # IDEMPOTENT WRITE BY RUN_ID
        # ----------------------------------------------------

        if spark.catalog.tableExists(
            MONITORING_TABLE
        ):

            spark.sql(
                f"""
                DELETE FROM {MONITORING_TABLE}
                WHERE run_id = '{run_id}'
                """
            )

            (
                monitoring
                .writeTo(
                    MONITORING_TABLE
                )
                .append()
            )

        else:

            (
                monitoring
                .writeTo(
                    MONITORING_TABLE
                )
                .using("iceberg")
                .create()
            )

        print()
        print(
            "Monitoring table:",
            MONITORING_TABLE,
        )

        print()
        print("=" * 70)
        print("MONITORING SUCCESS")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()