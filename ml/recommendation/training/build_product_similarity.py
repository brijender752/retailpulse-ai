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
    MIN_PRODUCT_CUSTOMERS,
    MIN_CO_INTERACTIONS,
    TOP_SIMILAR_PRODUCTS,
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Product Similarity"
    )

    try:

        print()
        print("=" * 70)
        print("ML-2C PRODUCT SIMILARITY")
        print("=" * 70)

        # ====================================================
        # LOAD INTERACTIONS
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
                F.col("customer_id").isNotNull()
                &
                F.col("product_id").isNotNull()
                &
                (
                    F.col(
                        "interaction_score"
                    ) > 0
                )
            )
        )

        interaction_count = (
            interactions.count()
        )

        if interaction_count == 0:

            raise RuntimeError(
                "No customer-product interactions found."
            )

        print(
            "Interaction rows:",
            f"{interaction_count:,}",
        )

        # ====================================================
        # PRODUCT CUSTOMER COUNTS
        #
        # Number of unique customers that interacted
        # with each product.
        # ====================================================

        product_counts = (
            interactions
            .groupBy(
                "product_id"
            )
            .agg(
                F.countDistinct(
                    "customer_id"
                ).alias(
                    "product_customer_count"
                )
            )
            .filter(
                F.col(
                    "product_customer_count"
                )
                >=
                MIN_PRODUCT_CUSTOMERS
            )
        )

        eligible_products = (
            product_counts.count()
        )

        print(
            "Eligible products:",
            f"{eligible_products:,}",
        )

        if eligible_products < 2:

            raise RuntimeError(
                "Not enough products have sufficient "
                "customer interactions."
            )

        # ====================================================
        # FILTER INTERACTIONS
        # ====================================================

        eligible_interactions = (
            interactions
            .join(
                product_counts.select(
                    "product_id"
                ),
                "product_id",
                "inner",
            )
        )

        # ====================================================
        # BINARY CUSTOMER-PRODUCT PAIRS
        #
        # Similarity initially uses whether the customer
        # interacted with a product.
        #
        # We keep interaction_score for recommendation
        # candidate scoring later.
        # ====================================================

        binary_interactions = (
            eligible_interactions
            .select(
                "customer_id",
                "product_id",
            )
            .distinct()
        )

        # ====================================================
        # SELF JOIN
        #
        # Customer:
        #
        # P1
        # P2
        # P3
        #
        # becomes:
        #
        # P1 P2
        # P1 P3
        # P2 P3
        #
        # product_a < product_b prevents:
        #
        # P1 P2
        # P2 P1
        #
        # from both being generated here.
        # ====================================================

        a = binary_interactions.alias("a")
        b = binary_interactions.alias("b")

        pairs = (
            a.join(
                b,
                (
                    F.col(
                        "a.customer_id"
                    )
                    ==
                    F.col(
                        "b.customer_id"
                    )
                )
                &
                (
                    F.col(
                        "a.product_id"
                    )
                    <
                    F.col(
                        "b.product_id"
                    )
                ),
                "inner",
            )
            .select(
                F.col(
                    "a.customer_id"
                ).alias(
                    "customer_id"
                ),

                F.col(
                    "a.product_id"
                ).alias(
                    "product_a"
                ),

                F.col(
                    "b.product_id"
                ).alias(
                    "product_b"
                ),
            )
        )

        # ====================================================
        # CO-INTERACTION COUNTS
        # ====================================================

        pair_counts = (
            pairs
            .groupBy(
                "product_a",
                "product_b",
            )
            .agg(
                F.countDistinct(
                    "customer_id"
                ).alias(
                    "co_interaction_count"
                )
            )
            .filter(
                F.col(
                    "co_interaction_count"
                )
                >=
                MIN_CO_INTERACTIONS
            )
        )

        # ====================================================
        # ATTACH INDIVIDUAL PRODUCT COUNTS
        # ====================================================

        counts_a = (
            product_counts
            .select(
                F.col(
                    "product_id"
                ).alias(
                    "product_a"
                ),

                F.col(
                    "product_customer_count"
                ).alias(
                    "customers_a"
                ),
            )
        )

        counts_b = (
            product_counts
            .select(
                F.col(
                    "product_id"
                ).alias(
                    "product_b"
                ),

                F.col(
                    "product_customer_count"
                ).alias(
                    "customers_b"
                ),
            )
        )

        similarity = (
            pair_counts
            .join(
                counts_a,
                "product_a",
                "inner",
            )
            .join(
                counts_b,
                "product_b",
                "inner",
            )
        )

        # ====================================================
        # COSINE SIMILARITY
        #
        # co_customers
        # ------------------------------
        # sqrt(customers_a * customers_b)
        # ====================================================

        similarity = (
            similarity
            .withColumn(
                "similarity_score",

                F.col(
                    "co_interaction_count"
                ).cast("double")

                /

                F.sqrt(
                    F.col(
                        "customers_a"
                    ).cast("double")
                    *
                    F.col(
                        "customers_b"
                    ).cast("double")
                ),
            )
        )

        # ====================================================
        # ALSO CALCULATE JACCARD
        #
        # Useful for evaluation/debugging.
        #
        # intersection
        # -------------------
        # A + B - intersection
        # ====================================================

        similarity = (
            similarity
            .withColumn(
                "jaccard_score",

                F.col(
                    "co_interaction_count"
                ).cast("double")

                /

                (
                    F.col(
                        "customers_a"
                    )
                    +
                    F.col(
                        "customers_b"
                    )
                    -
                    F.col(
                        "co_interaction_count"
                    )
                ).cast("double"),
            )
        )

        # ====================================================
        # CREATE BOTH DIRECTIONS
        #
        # Original:
        #
        # A → B
        #
        # Also create:
        #
        # B → A
        #
        # because inference starts from a source product.
        # ====================================================

        forward = (
            similarity
            .select(
                F.col(
                    "product_a"
                ).alias(
                    "source_product_id"
                ),

                F.col(
                    "product_b"
                ).alias(
                    "similar_product_id"
                ),

                "co_interaction_count",

                F.col(
                    "customers_a"
                ).alias(
                    "source_customer_count"
                ),

                F.col(
                    "customers_b"
                ).alias(
                    "similar_customer_count"
                ),

                "similarity_score",
                "jaccard_score",
            )
        )

        reverse = (
            similarity
            .select(
                F.col(
                    "product_b"
                ).alias(
                    "source_product_id"
                ),

                F.col(
                    "product_a"
                ).alias(
                    "similar_product_id"
                ),

                "co_interaction_count",

                F.col(
                    "customers_b"
                ).alias(
                    "source_customer_count"
                ),

                F.col(
                    "customers_a"
                ).alias(
                    "similar_customer_count"
                ),

                "similarity_score",
                "jaccard_score",
            )
        )

        similarity = (
            forward
            .unionByName(
                reverse
            )
        )

        # ====================================================
        # REMOVE INVALID SCORES
        # ====================================================

        similarity = (
            similarity
            .filter(
                F.col(
                    "similarity_score"
                ).between(
                    0.0,
                    1.0,
                )
            )
        )

        # ====================================================
        # KEEP TOP SIMILAR PRODUCTS
        #
        # Prevent the similarity table from growing
        # unnecessarily.
        # ====================================================

        similarity_window = (
            Window
            .partitionBy(
                "source_product_id"
            )
            .orderBy(
                F.desc(
                    "similarity_score"
                ),
                F.desc(
                    "co_interaction_count"
                ),
                F.asc(
                    "similar_product_id"
                ),
            )
        )

        similarity = (
            similarity
            .withColumn(
                "similarity_rank",

                F.row_number().over(
                    similarity_window
                ),
            )
            .filter(
                F.col(
                    "similarity_rank"
                )
                <=
                TOP_SIMILAR_PRODUCTS
            )
        )

        # ====================================================
        # METADATA
        # ====================================================

        similarity = (
            similarity
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

        # ====================================================
        # VALIDATION
        # ====================================================

        similarity_count = (
            similarity.count()
        )

        print()
        print("=" * 70)
        print("SIMILARITY STATISTICS")
        print("=" * 70)

        print(
            "Similarity rows:",
            f"{similarity_count:,}",
        )

        if similarity_count == 0:

            raise RuntimeError(
                "No product similarity pairs generated. "
                "Try lowering MIN_CO_INTERACTIONS."
            )

        (
            similarity
            .orderBy(
                F.desc(
                    "similarity_score"
                ),
                F.desc(
                    "co_interaction_count"
                ),
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
            similarity
            .writeTo(
                PRODUCT_SIMILARITY_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print("=" * 70)
        print("SUCCESS")
        print("=" * 70)

        print(
            PRODUCT_SIMILARITY_TABLE
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()