from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import Window
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


# ============================================================
# TABLES
# ============================================================

CUSTOMERS_TABLE = (
    "retailpulse.analytics.dim_customer"
)

PRODUCTS_TABLE = (
    "retailpulse.silver.products"
)

INTERACTIONS_TABLE = (
    "retailpulse.ml.customer_product_interactions"
)

POPULARITY_TABLE = (
    "retailpulse.ml.product_popularity"
)

SIMILARITY_TABLE = (
    "retailpulse.ml.product_similarity"
)

SELECTED_CONFIG_TABLE = (
    "retailpulse.ml.recommendation_selected_config"
)

OUTPUT_TABLE = (
    "retailpulse.ml.product_recommendations"
)

RUN_HISTORY_TABLE = (
    "retailpulse.ml.recommendation_run_history"
)


TOP_K = 10

CANDIDATE_LIMIT = 100


# ============================================================
# MAIN
# ============================================================

def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Production Recommendations"
    )

    run_id = str(
        uuid.uuid4()
    )

    started_at = datetime.now(
        timezone.utc
    )

    print()
    print("=" * 70)
    print("ML-2H PRODUCTION RECOMMENDATION GENERATION")
    print("=" * 70)

    print()
    print(
        "Run ID:",
        run_id,
    )

    try:

        # ====================================================
        # LOAD CHAMPION CONFIG
        # ====================================================

        config_rows = (
            spark.table(
                SELECTED_CONFIG_TABLE
            )
            .collect()
        )

        if len(config_rows) != 1:

            raise RuntimeError(
                "Expected exactly one selected "
                "recommendation configuration. "
                f"Found {len(config_rows)}."
            )

        config = config_rows[0]

        model_name = str(
            config["model"]
        )

        personalized_weight = float(
            config[
                "personalized_weight"
            ]
        )

        popularity_weight = float(
            config[
                "popularity_weight"
            ]
        )

        print()
        print("Champion configuration")

        print(
            "Model:",
            model_name,
        )

        print(
            "Personalized weight:",
            personalized_weight,
        )

        print(
            "Popularity weight:",
            popularity_weight,
        )

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
            .filter(
                F.col(
                    "customer_id"
                ).isNotNull()
            )
            .distinct()
        )

        total_customers = (
            customers.count()
        )

        if total_customers == 0:

            raise RuntimeError(
                "Customer table is empty."
            )

        print()
        print(
            "Customers:",
            f"{total_customers:,}",
        )

        # ====================================================
        # INTERACTIONS
        # ====================================================

        interactions = (
            spark.table(
                INTERACTIONS_TABLE
            )
            .select(
                "customer_id",
                "product_id",
                "interaction_score",
            )
            .filter(
                F.col(
                    "customer_id"
                ).isNotNull()
                &
                F.col(
                    "product_id"
                ).isNotNull()
                &
                (
                    F.col(
                        "interaction_score"
                    ) > 0
                )
            )
        )

        seen = (
            interactions
            .select(
                F.col(
                    "customer_id"
                ).alias(
                    "seen_customer_id"
                ),

                F.col(
                    "product_id"
                ).alias(
                    "seen_product_id"
                ),
            )
            .distinct()
        )

        customers_with_history = (
            interactions
            .select(
                "customer_id"
            )
            .distinct()
        )

        history_count = (
            customers_with_history
            .count()
        )

        cold_start_count = (
            total_customers
            -
            history_count
        )

        print(
            "Customers with history:",
            f"{history_count:,}",
        )

        print(
            "Cold-start customers:",
            f"{cold_start_count:,}",
        )

        # ====================================================
        # PRODUCT SIMILARITY
        # ====================================================

        similarity = (
            spark.table(
                SIMILARITY_TABLE
            )
            .select(
                "source_product_id",
                "similar_product_id",
                "similarity_score",
            )
            .filter(
                F.col(
                    "similarity_score"
                ) > 0
            )
        )

        # ====================================================
        # PERSONALIZED CANDIDATES
        # ====================================================

        personalized = (
            interactions.alias("i")
            .join(
                similarity.alias("s"),

                F.col(
                    "i.product_id"
                )
                ==
                F.col(
                    "s.source_product_id"
                ),

                "inner",
            )
            .select(
                F.col(
                    "i.customer_id"
                ).alias(
                    "customer_id"
                ),

                F.col(
                    "s.similar_product_id"
                ).alias(
                    "product_id"
                ),

                F.col(
                    "i.product_id"
                ).alias(
                    "source_product_id"
                ),

                F.col(
                    "s.similarity_score"
                ).alias(
                    "similarity_score"
                ),

                (
                    F.col(
                        "i.interaction_score"
                    )
                    *
                    F.col(
                        "s.similarity_score"
                    )
                ).alias(
                    "contribution"
                ),
            )
        )

        personalized = (
            personalized
            .groupBy(
                "customer_id",
                "product_id",
            )
            .agg(
                F.sum(
                    "contribution"
                ).alias(
                    "raw_personalized_score"
                ),

                F.max(
                    "similarity_score"
                ).alias(
                    "max_similarity"
                ),

                F.countDistinct(
                    "source_product_id"
                ).alias(
                    "supporting_products"
                ),

                F.max_by(
                    "source_product_id",
                    "contribution",
                ).alias(
                    "primary_source_product_id"
                ),
            )
        )

        # ====================================================
        # REMOVE SEEN PRODUCTS
        # ====================================================

        personalized = (
            personalized
            .join(
                seen,

                (
                    F.col(
                        "customer_id"
                    )
                    ==
                    F.col(
                        "seen_customer_id"
                    )
                )
                &
                (
                    F.col(
                        "product_id"
                    )
                    ==
                    F.col(
                        "seen_product_id"
                    )
                ),

                "left_anti",
            )
        )

        # ====================================================
        # NORMALIZE PERSONALIZED SCORES
        # ====================================================

        customer_window = (
            Window.partitionBy(
                "customer_id"
            )
        )

        personalized = (
            personalized
            .withColumn(
                "max_raw_score",

                F.max(
                    "raw_personalized_score"
                ).over(
                    customer_window
                ),
            )
            .withColumn(
                "personalized_score",

                F.when(
                    F.col(
                        "max_raw_score"
                    ) > 0,

                    F.col(
                        "raw_personalized_score"
                    )
                    /
                    F.col(
                        "max_raw_score"
                    ),

                ).otherwise(
                    F.lit(0.0)
                ),
            )
            .drop(
                "max_raw_score"
            )
        )

        # ====================================================
        # POPULARITY
        # ====================================================

        popularity = (
            spark.table(
                POPULARITY_TABLE
            )
            .select(
                "product_id",
                "popularity_score",
                "global_rank",
            )
            .filter(
                F.col(
                    "global_rank"
                )
                <=
                CANDIDATE_LIMIT
            )
        )

        if popularity.limit(1).count() == 0:

            raise RuntimeError(
                "Product popularity table is empty."
            )

        # ====================================================
        # PERSONALIZED + POPULARITY
        # ====================================================

        personalized_candidates = (
            personalized.alias("p")
            .join(
                popularity.alias("pop"),
                "product_id",
                "left",
            )
            .select(
                "customer_id",
                "product_id",

                "personalized_score",

                F.coalesce(
                    F.col(
                        "popularity_score"
                    ),
                    F.lit(0.0),
                ).alias(
                    "popularity_score"
                ),

                "max_similarity",
                "supporting_products",
                "primary_source_product_id",

                F.lit(
                    True
                ).alias(
                    "is_personalized"
                ),
            )
        )

        # ====================================================
        # POPULARITY FALLBACK FOR ALL CUSTOMERS
        # ====================================================

        popularity_candidates = (
            customers
            .crossJoin(
                popularity.select(
                    "product_id",
                    "popularity_score",
                )
            )
            .join(
                seen,

                (
                    F.col(
                        "customer_id"
                    )
                    ==
                    F.col(
                        "seen_customer_id"
                    )
                )
                &
                (
                    F.col(
                        "product_id"
                    )
                    ==
                    F.col(
                        "seen_product_id"
                    )
                ),

                "left_anti",
            )
            .select(
                "customer_id",
                "product_id",

                F.lit(
                    0.0
                ).alias(
                    "personalized_score"
                ),

                "popularity_score",

                F.lit(
                    None
                ).cast(
                    "double"
                ).alias(
                    "max_similarity"
                ),

                F.lit(
                    0
                ).cast(
                    "long"
                ).alias(
                    "supporting_products"
                ),

                F.lit(
                    None
                ).cast(
                    interactions.schema[
                        "product_id"
                    ].dataType
                ).alias(
                    "primary_source_product_id"
                ),

                F.lit(
                    False
                ).alias(
                    "is_personalized"
                ),
            )
        )

        # ====================================================
        # UNION / DEDUPLICATE
        # ====================================================

        candidates = (
            personalized_candidates
            .unionByName(
                popularity_candidates
            )
            .groupBy(
                "customer_id",
                "product_id",
            )
            .agg(
                F.max(
                    "personalized_score"
                ).alias(
                    "personalized_score"
                ),

                F.max(
                    "popularity_score"
                ).alias(
                    "popularity_score"
                ),

                F.max(
                    "max_similarity"
                ).alias(
                    "max_similarity"
                ),

                F.max(
                    "supporting_products"
                ).alias(
                    "supporting_products"
                ),

                F.max(
                    "primary_source_product_id"
                ).alias(
                    "primary_source_product_id"
                ),

                F.max(
                    F.col(
                        "is_personalized"
                    ).cast("int")
                ).alias(
                    "is_personalized_int"
                ),
            )
            .withColumn(
                "is_personalized",

                F.col(
                    "is_personalized_int"
                ) == 1,
            )
            .drop(
                "is_personalized_int"
            )
        )

        # ====================================================
        # CHAMPION HYBRID SCORE
        # ====================================================

        candidates = (
            candidates
            .withColumn(
                "recommendation_score",

                (
                    F.col(
                        "personalized_score"
                    )
                    *
                    F.lit(
                        personalized_weight
                    )
                )
                +
                (
                    F.col(
                        "popularity_score"
                    )
                    *
                    F.lit(
                        popularity_weight
                    )
                ),
            )
        )

        # ====================================================
        # DETERMINE RECOMMENDATION REASON
        # ====================================================

        candidates = (
            candidates.alias("c")
            .join(
                customers_with_history
                .withColumn(
                    "has_history",
                    F.lit(True),
                )
                .alias("h"),

                "customer_id",
                "left",
            )
            .withColumn(
                "has_history",

                F.coalesce(
                    F.col(
                        "has_history"
                    ),
                    F.lit(False),
                ),
            )
            .withColumn(
                "recommendation_reason",

                F.when(
                    ~F.col(
                        "has_history"
                    ),

                    F.lit(
                        "popular_cold_start"
                    ),

                ).when(
                    F.col(
                        "is_personalized"
                    )
                    &
                    (
                        F.col(
                            "popularity_score"
                        ) > 0
                    ),

                    F.lit(
                        "hybrid_personalized"
                    ),

                ).when(
                    F.col(
                        "is_personalized"
                    ),

                    F.lit(
                        "personalized_similarity"
                    ),

                ).otherwise(
                    F.lit(
                        "popular_fallback"
                    )
                ),
            )
        )

        # ====================================================
        # TOP K
        # ====================================================

        ranking_window = (
            Window
            .partitionBy(
                "customer_id"
            )
            .orderBy(
                F.desc(
                    "recommendation_score"
                ),

                F.desc(
                    "personalized_score"
                ),

                F.desc(
                    "popularity_score"
                ),

                F.asc(
                    "product_id"
                ),
            )
        )

        recommendations = (
            candidates
            .withColumn(
                "recommendation_rank",

                F.row_number().over(
                    ranking_window
                ),
            )
            .filter(
                F.col(
                    "recommendation_rank"
                )
                <=
                TOP_K
            )
        )

        # ====================================================
        # PRODUCT METADATA
        # ====================================================

        products = (
            spark.table(
                PRODUCTS_TABLE
            )
        )

        available_metadata = [
            column
            for column in [
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "price",
            ]
            if column in products.columns
        ]

        products = (
            products
            .select(
                *available_metadata
            )
            .dropDuplicates(
                ["product_id"]
            )
        )

        recommendations = (
            recommendations
            .join(
                products,
                "product_id",
                "left",
            )
        )

        # ====================================================
        # RUN METADATA
        # ====================================================

        recommendations = (
            recommendations
            .withColumn(
                "run_id",
                F.lit(
                    run_id
                ),
            )
            .withColumn(
                "model_name",
                F.lit(
                    model_name
                ),
            )
            .withColumn(
                "model_version",
                F.lit(
                    "v1"
                ),
            )
            .withColumn(
                "personalized_weight",
                F.lit(
                    personalized_weight
                ),
            )
            .withColumn(
                "popularity_weight",
                F.lit(
                    popularity_weight
                ),
            )
            .withColumn(
                "generated_at",
                F.current_timestamp(),
            )
        )

        # ====================================================
        # VALIDATION
        # ====================================================

        recommendation_count = (
            recommendations.count()
        )

        recommended_customers = (
            recommendations
            .select(
                "customer_id"
            )
            .distinct()
            .count()
        )

        if recommendation_count == 0:

            raise RuntimeError(
                "Production recommender generated "
                "zero recommendations."
            )

        duplicate_count = (
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

        if duplicate_count:

            raise RuntimeError(
                "Duplicate customer-product "
                "recommendations detected."
            )

        over_top_k = (
            recommendations
            .groupBy(
                "customer_id"
            )
            .count()
            .filter(
                F.col("count") > TOP_K
            )
            .count()
        )

        if over_top_k:

            raise RuntimeError(
                "Customers with more than "
                f"{TOP_K} recommendations detected."
            )

        seen_leakage = (
            recommendations.alias("r")
            .join(
                seen.alias("s"),

                (
                    F.col(
                        "r.customer_id"
                    )
                    ==
                    F.col(
                        "s.seen_customer_id"
                    )
                )
                &
                (
                    F.col(
                        "r.product_id"
                    )
                    ==
                    F.col(
                        "s.seen_product_id"
                    )
                ),

                "inner",
            )
            .count()
        )

        if seen_leakage:

            raise RuntimeError(
                "Previously seen products appeared "
                "in recommendations."
            )

        # ====================================================
        # CURRENT SERVING SNAPSHOT
        # ====================================================

        (
            recommendations
            .writeTo(
                OUTPUT_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        # ====================================================
        # RUN HISTORY
        # ====================================================

        reason_counts = (
            recommendations
            .groupBy(
                "recommendation_reason"
            )
            .count()
            .collect()
        )

        reason_map = {
            row[
                "recommendation_reason"
            ]: int(
                row["count"]
            )
            for row in reason_counts
        }

        finished_at = datetime.now(
            timezone.utc
        )

        duration_seconds = (
            finished_at
            -
            started_at
        ).total_seconds()

        run_history = (
            spark.createDataFrame(
                [
                    (
                        run_id,
                        model_name,
                        "v1",
                        personalized_weight,
                        popularity_weight,
                        int(
                            total_customers
                        ),
                        int(
                            history_count
                        ),
                        int(
                            cold_start_count
                        ),
                        int(
                            recommended_customers
                        ),
                        int(
                            recommendation_count
                        ),
                        int(
                            reason_map.get(
                                "hybrid_personalized",
                                0,
                            )
                        ),
                        int(
                            reason_map.get(
                                "personalized_similarity",
                                0,
                            )
                        ),
                        int(
                            reason_map.get(
                                "popular_fallback",
                                0,
                            )
                        ),
                        int(
                            reason_map.get(
                                "popular_cold_start",
                                0,
                            )
                        ),
                        float(
                            duration_seconds
                        ),
                        started_at,
                        finished_at,
                        "SUCCESS",
                    )
                ],
                """
                run_id string,
                model_name string,
                model_version string,
                personalized_weight double,
                popularity_weight double,
                total_customers long,
                customers_with_history long,
                cold_start_customers long,
                recommended_customers long,
                recommendation_count long,
                hybrid_personalized_count long,
                personalized_similarity_count long,
                popular_fallback_count long,
                popular_cold_start_count long,
                duration_seconds double,
                started_at timestamp,
                finished_at timestamp,
                status string
                """,
            )
        )

        if spark.catalog.tableExists(
            RUN_HISTORY_TABLE
        ):

            (
                run_history
                .writeTo(
                    RUN_HISTORY_TABLE
                )
                .append()
            )

        else:

            (
                run_history
                .writeTo(
                    RUN_HISTORY_TABLE
                )
                .using("iceberg")
                .create()
            )

        print()
        print("=" * 70)
        print("PRODUCTION RECOMMENDATIONS COMPLETE")
        print("=" * 70)

        print(
            "Run ID:",
            run_id,
        )

        print(
            "Recommendations:",
            f"{recommendation_count:,}",
        )

        print(
            "Customers served:",
            f"{recommended_customers:,}",
        )

        print(
            "Customer coverage:",
            round(
                recommended_customers
                /
                total_customers,
                4,
            ),
        )

        print(
            "Duration:",
            round(
                duration_seconds,
                2,
            ),
            "seconds",
        )

        print()
        print(
            "Serving table:",
            OUTPUT_TABLE,
        )

        print(
            "History table:",
            RUN_HISTORY_TABLE,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()