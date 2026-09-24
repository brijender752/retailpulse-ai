from __future__ import annotations

import sys

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


from iceberg_session import (
    create_iceberg_spark_session,
)

from common.ml_config import (
    RECOMMENDATION_INTERACTIONS_TABLE,
    ITEM_ITEM_RECOMMENDATIONS_TABLE,
    PRODUCT_POPULARITY_TABLE,
    PRODUCT_RECOMMENDATIONS_TABLE,
    HYBRID_PERSONALIZED_WEIGHT,
    HYBRID_POPULARITY_WEIGHT,
    HYBRID_CANDIDATE_LIMIT,
    TOP_RECOMMENDATIONS,
)


CUSTOMERS_TABLE = (
    "retailpulse.analytics.dim_customer"
)

PRODUCTS_TABLE = (
    "retailpulse.silver.products"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Hybrid Recommendations"
    )

    try:

        print()
        print("=" * 70)
        print("ML-2D HYBRID RECOMMENDER")
        print("=" * 70)

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

        customer_count = (
            customers.count()
        )

        print(
            "Customers:",
            f"{customer_count:,}",
        )

        if customer_count == 0:

            raise RuntimeError(
                "No customers found."
            )

        # ====================================================
        # CUSTOMER HISTORY
        # ====================================================

        interactions = (
            spark.table(
                RECOMMENDATION_INTERACTIONS_TABLE
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
            )
        )

        customers_with_history = (
            interactions
            .select(
                "customer_id"
            )
            .distinct()
            .withColumn(
                "has_history",
                F.lit(1),
            )
        )

        # ====================================================
        # PERSONALIZED CANDIDATES
        # ====================================================

        personalized = (
            spark.table(
                ITEM_ITEM_RECOMMENDATIONS_TABLE
            )
            .select(
                "customer_id",
                "product_id",

                F.col(
                    "recommendation_score"
                )
                .cast("double")
                .alias(
                    "raw_personalized_score"
                ),

                F.col(
                    "max_similarity"
                )
                .cast("double")
                .alias(
                    "max_similarity"
                ),

                "supporting_products",
                "primary_source_product_id",
            )
        )

        # ====================================================
        # NORMALIZE PERSONALIZED SCORE PER CUSTOMER
        #
        # Raw item-item recommendation scores are not on
        # the same scale as popularity scores.
        #
        # Normalize to 0..1 before combining.
        # ====================================================

        customer_score_window = (
            Window.partitionBy(
                "customer_id"
            )
        )

        personalized = (
            personalized
            .withColumn(
                "max_customer_personalized_score",

                F.max(
                    "raw_personalized_score"
                ).over(
                    customer_score_window
                ),
            )
            .withColumn(
                "personalized_score",

                F.when(
                    F.col(
                        "max_customer_personalized_score"
                    ) > 0,

                    F.col(
                        "raw_personalized_score"
                    )
                    /
                    F.col(
                        "max_customer_personalized_score"
                    ),

                ).otherwise(
                    F.lit(0.0)
                ),
            )
            .drop(
                "max_customer_personalized_score"
            )
        )

        # ====================================================
        # POPULARITY CANDIDATES
        #
        # We don't need every product for every customer.
        # Keep only top global candidates.
        # ====================================================

        popularity = (
            spark.table(
                PRODUCT_POPULARITY_TABLE
            )
            .filter(
                F.col(
                    "global_rank"
                )
                <=
                HYBRID_CANDIDATE_LIMIT
            )
            .select(
                "product_id",

                F.col(
                    "popularity_score"
                )
                .cast("double")
                .alias(
                    "popularity_score"
                ),

                "global_rank",
            )
        )

        popular_product_count = (
            popularity.count()
        )

        if popular_product_count == 0:

            raise RuntimeError(
                "Product popularity table "
                "contains no candidates."
            )

        print(
            "Popularity candidates:",
            f"{popular_product_count:,}",
        )

        # ====================================================
        # PERSONALIZED + POPULARITY SCORE
        #
        # For personalized candidates, attach their global
        # popularity score.
        #
        # Products missing from the top popularity candidate
        # set receive popularity score 0.
        # ====================================================

        personalized_candidates = (
            personalized.alias("p")
            .join(
                popularity.alias("pop"),

                F.col(
                    "p.product_id"
                )
                ==
                F.col(
                    "pop.product_id"
                ),

                "left",
            )
            .select(
                F.col(
                    "p.customer_id"
                ).alias(
                    "customer_id"
                ),

                F.col(
                    "p.product_id"
                ).alias(
                    "product_id"
                ),

                F.col(
                    "p.raw_personalized_score"
                ).alias(
                    "raw_personalized_score"
                ),

                F.col(
                    "p.personalized_score"
                ).alias(
                    "personalized_score"
                ),

                F.coalesce(
                    F.col(
                        "pop.popularity_score"
                    ),
                    F.lit(0.0),
                ).alias(
                    "popularity_score"
                ),

                F.col(
                    "p.max_similarity"
                ).alias(
                    "max_similarity"
                ),

                F.col(
                    "p.supporting_products"
                ).alias(
                    "supporting_products"
                ),

                F.col(
                    "p.primary_source_product_id"
                ).alias(
                    "primary_source_product_id"
                ),

                F.lit(
                    "personalized"
                ).alias(
                    "candidate_source"
                ),
            )
        )

        # ====================================================
        # POPULARITY FALLBACK CANDIDATES
        #
        # Create global popular candidates for every customer.
        #
        # This does two things:
        #
        # 1. solves cold start
        # 2. fills fewer-than-10 personalized recommendations
        # ====================================================

        popularity_candidates = (
            customers
            .crossJoin(
                popularity
            )
            .select(
                "customer_id",
                "product_id",

                F.lit(
                    0.0
                ).alias(
                    "raw_personalized_score"
                ),

                F.lit(
                    0.0
                ).alias(
                    "personalized_score"
                ),

                "popularity_score",

                F.lit(
                    0.0
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
                    "popularity"
                ).alias(
                    "candidate_source"
                ),
            )
        )

        # ====================================================
        # UNION CANDIDATES
        # ====================================================

        candidates = (
            personalized_candidates
            .unionByName(
                popularity_candidates
            )
        )

        # ====================================================
        # REMOVE ALREADY-SEEN PRODUCTS
        # ====================================================

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

        candidates = (
            candidates
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
        # MERGE DUPLICATE CANDIDATES
        #
        # A product can arrive through:
        #
        # personalized
        # popularity
        #
        # We need one customer-product candidate.
        # ====================================================

        candidates = (
            candidates
            .groupBy(
                "customer_id",
                "product_id",
            )
            .agg(

                F.max(
                    "raw_personalized_score"
                ).alias(
                    "raw_personalized_score"
                ),

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

                F.max_by(
                    "primary_source_product_id",
                    "raw_personalized_score",
                ).alias(
                    "primary_source_product_id"
                ),

                F.max(
                    F.when(
                        F.col(
                            "candidate_source"
                        )
                        ==
                        "personalized",
                        F.lit(1),
                    ).otherwise(
                        F.lit(0),
                    )
                ).alias(
                    "has_personalized_candidate"
                ),
            )
        )

        # ====================================================
        # CUSTOMER HISTORY FLAG
        # ====================================================

        candidates = (
            candidates
            .join(
                customers_with_history,
                "customer_id",
                "left",
            )
            .withColumn(
                "has_history",
                F.coalesce(
                    F.col(
                        "has_history"
                    ),
                    F.lit(0),
                ),
            )
        )

        # ====================================================
        # HYBRID SCORE
        #
        # Existing customer + personalized candidate:
        #
        # 80% personalized
        # 20% popularity
        #
        # Popular fallback candidate:
        #
        # popularity only
        #
        # Cold-start customer:
        #
        # popularity only
        # ====================================================

        candidates = (
            candidates
            .withColumn(
                "hybrid_score",

                F.when(
                    F.col(
                        "has_personalized_candidate"
                    ) == 1,

                    (
                        F.col(
                            "personalized_score"
                        )
                        *
                        F.lit(
                            HYBRID_PERSONALIZED_WEIGHT
                        )
                    )
                    +
                    (
                        F.col(
                            "popularity_score"
                        )
                        *
                        F.lit(
                            HYBRID_POPULARITY_WEIGHT
                        )
                    ),

                ).otherwise(
                    F.col(
                        "popularity_score"
                    )
                ),
            )
        )

        # ====================================================
        # RECOMMENDATION REASON
        # ====================================================

        candidates = (
            candidates
            .withColumn(
                "recommendation_reason",

                F.when(
                    F.col(
                        "has_history"
                    ) == 0,

                    F.lit(
                        "popular_cold_start"
                    ),

                ).when(
                    F.col(
                        "has_personalized_candidate"
                    ) == 1,

                    F.when(
                        F.col(
                            "popularity_score"
                        ) > 0,

                        F.lit(
                            "hybrid_personalized"
                        ),

                    ).otherwise(
                        F.lit(
                            "personalized_similarity"
                        )
                    ),

                ).otherwise(
                    F.lit(
                        "popular_fallback"
                    )
                ),
            )
        )

        # ====================================================
        # RANK TOP-N PER CUSTOMER
        # ====================================================

        rank_window = (
            Window
            .partitionBy(
                "customer_id"
            )
            .orderBy(
                F.desc(
                    "hybrid_score"
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
                    rank_window
                ),
            )
            .filter(
                F.col(
                    "recommendation_rank"
                )
                <=
                TOP_RECOMMENDATIONS
            )
        )

        # ====================================================
        # PRODUCT METADATA
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

        recommendations = (
            recommendations
            .join(
                products,
                "product_id",
                "left",
            )
        )

        # ====================================================
        # FINAL METADATA
        # ====================================================

        recommendations = (
            recommendations
            .withColumn(
                "recommendation_score",
                F.col(
                    "hybrid_score"
                ),
            )
            .withColumn(
                "model_name",
                F.lit(
                    "hybrid_item_item_popularity"
                ),
            )
            .withColumn(
                "model_version",
                F.lit(
                    "v1"
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
                "subcategory",
                "brand",
                "price",

                "recommendation_rank",
                "recommendation_score",

                "personalized_score",
                "popularity_score",
                "hybrid_score",

                "max_similarity",
                "supporting_products",
                "primary_source_product_id",

                "has_history",
                "recommendation_reason",

                "model_name",
                "model_version",
                "generated_at",
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

        print()
        print("=" * 70)
        print("HYBRID RECOMMENDATION STATISTICS")
        print("=" * 70)

        print(
            "Recommendations:",
            f"{recommendation_count:,}",
        )

        print(
            "Customers:",
            f"{recommended_customers:,}",
        )

        coverage = (
            recommended_customers
            /
            customer_count
            if customer_count
            else 0
        )

        print(
            "Customer coverage:",
            f"{coverage * 100:.2f}%",
        )

        # ====================================================
        # DUPLICATE VALIDATION
        # ====================================================

        duplicates = (
            recommendations
            .groupBy(
                "customer_id",
                "product_id",
            )
            .count()
            .filter(
                F.col(
                    "count"
                ) > 1
            )
            .count()
        )

        if duplicates > 0:

            raise RuntimeError(
                f"{duplicates} duplicate "
                "customer-product recommendations found."
            )

        # ====================================================
        # TOP-N VALIDATION
        # ====================================================

        too_many = (
            recommendations
            .groupBy(
                "customer_id"
            )
            .count()
            .filter(
                F.col(
                    "count"
                )
                >
                TOP_RECOMMENDATIONS
            )
            .count()
        )

        if too_many > 0:

            raise RuntimeError(
                "Customers with more than "
                f"{TOP_RECOMMENDATIONS} "
                "recommendations found."
            )

        # ====================================================
        # SEEN-PRODUCT VALIDATION
        # ====================================================

        seen_recommendations = (
            recommendations.alias("r")
            .join(
                interactions.alias("i"),

                (
                    F.col(
                        "r.customer_id"
                    )
                    ==
                    F.col(
                        "i.customer_id"
                    )
                )
                &
                (
                    F.col(
                        "r.product_id"
                    )
                    ==
                    F.col(
                        "i.product_id"
                    )
                ),

                "inner",
            )
            .count()
        )

        if seen_recommendations > 0:

            raise RuntimeError(
                f"{seen_recommendations} seen "
                "products were recommended."
            )

        # ====================================================
        # SCORE VALIDATION
        # ====================================================

        invalid_scores = (
            recommendations
            .filter(
                F.col(
                    "recommendation_score"
                ).isNull()
                |
                (
                    F.col(
                        "recommendation_score"
                    )
                    < 0
                )
            )
            .count()
        )

        if invalid_scores > 0:

            raise RuntimeError(
                f"{invalid_scores} invalid "
                "recommendation scores."
            )

        # ====================================================
        # DISPLAY REASONS
        # ====================================================

        print()
        print("Recommendation reasons:")

        (
            recommendations
            .groupBy(
                "recommendation_reason"
            )
            .count()
            .orderBy(
                F.desc(
                    "count"
                )
            )
            .show(
                truncate=False
            )
        )

        print()
        print("Example recommendations:")

        (
            recommendations
            .orderBy(
                "customer_id",
                "recommendation_rank",
            )
            .show(
                30,
                truncate=False,
            )
        )

        # ====================================================
        # WRITE ICEBERG
        # ====================================================

        spark.sql(
            """
            CREATE NAMESPACE IF NOT EXISTS
            retailpulse.ml
            """
        )

        (
            recommendations
            .writeTo(
                PRODUCT_RECOMMENDATIONS_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print("=" * 70)
        print("SUCCESS")
        print("=" * 70)

        print(
            PRODUCT_RECOMMENDATIONS_TABLE
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()