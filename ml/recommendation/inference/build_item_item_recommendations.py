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
    PRODUCT_SIMILARITY_TABLE,
    ITEM_ITEM_RECOMMENDATIONS_TABLE,
    PRODUCTS_TABLE,
    TOP_RECOMMENDATIONS,
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Item Item Recommendations"
    )

    try:

        print()
        print("=" * 70)
        print("ML-2C PERSONALIZED RECOMMENDATIONS")
        print("=" * 70)

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
                    "interaction_score"
                ) > 0
            )
        )

        # ====================================================
        # PRODUCT SIMILARITY
        # ====================================================

        similarity = (
            spark.table(
                PRODUCT_SIMILARITY_TABLE
            )
            .select(
                "source_product_id",
                "similar_product_id",
                "similarity_score",
                "co_interaction_count",
            )
        )

        # ====================================================
        # GENERATE CANDIDATES
        #
        # recommendation contribution:
        #
        # interaction strength
        #       ×
        # product similarity
        # ====================================================

        candidates = (
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
                    "i.product_id"
                ).alias(
                    "source_product_id"
                ),

                F.col(
                    "s.similar_product_id"
                ).alias(
                    "candidate_product_id"
                ),

                F.col(
                    "i.interaction_score"
                ).alias(
                    "source_interaction_score"
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
                    "candidate_contribution"
                ),
            )
        )

        # ====================================================
        # REMOVE PRODUCTS CUSTOMER ALREADY INTERACTED WITH
        # ====================================================

        seen_products = (
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
        )

        candidates = (
            candidates
            .join(
                seen_products,

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
                        "candidate_product_id"
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
        # AGGREGATE CANDIDATES
        #
        # Multiple source products may recommend the same
        # candidate.
        # ====================================================

        recommendations = (
            candidates
            .groupBy(
                "customer_id",
                "candidate_product_id",
            )
            .agg(

                F.sum(
                    "candidate_contribution"
                ).alias(
                    "recommendation_score"
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
                    "candidate_contribution",
                ).alias(
                    "primary_source_product_id"
                ),
            )
        )

        # ====================================================
        # RANK PER CUSTOMER
        # ====================================================

        rank_window = (
            Window
            .partitionBy(
                "customer_id"
            )
            .orderBy(
                F.desc(
                    "recommendation_score"
                ),
                F.desc(
                    "max_similarity"
                ),
                F.asc(
                    "candidate_product_id"
                ),
            )
        )

        recommendations = (
            recommendations
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
                F.col(
                    "product_id"
                ).alias(
                    "candidate_product_id"
                ),

                "product_name",
                "category",
                "subcategory",
                "brand",

                F.col(
                    "price"
                ).cast(
                    "double"
                ).alias(
                    "price"
                ),
            )
        )

        recommendations = (
            recommendations
            .join(
                products,
                "candidate_product_id",
                "left",
            )
        )

        # ====================================================
        # METADATA
        # ====================================================

        recommendations = (
            recommendations
            .withColumnRenamed(
                "candidate_product_id",
                "product_id",
            )
            .withColumn(
                "recommendation_reason",
                F.lit(
                    "similar_to_customer_history"
                ),
            )
            .withColumn(
                "model_name",
                F.lit(
                    "item_item_cosine_v1"
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

                "recommendation_score",
                "recommendation_rank",

                "max_similarity",
                "supporting_products",
                "primary_source_product_id",

                "recommendation_reason",
                "model_name",
                "generated_at",
            )
        )

        # ====================================================
        # VALIDATION
        # ====================================================

        count = (
            recommendations.count()
        )

        customer_count = (
            recommendations
            .select(
                "customer_id"
            )
            .distinct()
            .count()
        )

        print()
        print(
            "Recommendation rows:",
            f"{count:,}",
        )

        print(
            "Customers with recommendations:",
            f"{customer_count:,}",
        )

        if count == 0:

            raise RuntimeError(
                "No personalized recommendations generated."
            )

        # Verify that we didn't recommend seen products.

        validation = (
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

        if validation > 0:

            raise RuntimeError(
                f"{validation} already-seen products "
                "were recommended."
            )

        recommendations.show(
            30,
            truncate=False,
        )

        # ====================================================
        # WRITE ICEBERG
        # ====================================================

        (
            recommendations
            .writeTo(
                ITEM_ITEM_RECOMMENDATIONS_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print("=" * 70)
        print("SUCCESS")
        print("=" * 70)

        print(
            ITEM_ITEM_RECOMMENDATIONS_TABLE
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()