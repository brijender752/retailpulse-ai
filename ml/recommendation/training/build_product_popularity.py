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


from iceberg_session import create_iceberg_spark_session

from common.ml_config import (
    RECOMMENDATION_INTERACTIONS_TABLE,
    PRODUCT_POPULARITY_TABLE,
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Product Popularity"
    )

    try:

        print()
        print("=" * 70)
        print("ML-2B PRODUCT POPULARITY")
        print("=" * 70)

        # ====================================================
        # LOAD INTERACTIONS
        # ====================================================

        interactions = spark.table(
            RECOMMENDATION_INTERACTIONS_TABLE
        )

        interaction_count = interactions.count()

        if interaction_count == 0:
            raise RuntimeError(
                "customer_product_interactions is empty."
            )

        print(
            "Interaction rows:",
            f"{interaction_count:,}",
        )

        # ====================================================
        # PRODUCT AGGREGATION
        # ====================================================

        popularity = (
            interactions
            .groupBy(
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "price",
            )
            .agg(

                F.countDistinct(
                    "customer_id"
                ).alias(
                    "unique_customers"
                ),

                F.sum(
                    "purchase_count"
                ).alias(
                    "total_purchases"
                ),

                F.sum(
                    "units_purchased"
                ).alias(
                    "total_units"
                ),

                F.sum(
                    "product_view_count"
                ).alias(
                    "total_views"
                ),

                F.sum(
                    "interaction_score"
                ).alias(
                    "total_interaction_score"
                ),

                F.max(
                    "last_interaction_at"
                ).alias(
                    "last_interaction_at"
                ),
            )
        )

        # ====================================================
        # NULL HANDLING
        # ====================================================

        numeric_columns = [
            "unique_customers",
            "total_purchases",
            "total_units",
            "total_views",
            "total_interaction_score",
        ]

        for column in numeric_columns:

            popularity = (
                popularity
                .withColumn(
                    column,
                    F.coalesce(
                        F.col(column),
                        F.lit(0),
                    ),
                )
            )

        # ====================================================
        # NORMALIZATION
        #
        # Raw purchase/view counts can have very different
        # scales. Normalize each metric to 0..1 before
        # combining them.
        # ====================================================

        stats = (
            popularity
            .agg(
                F.max(
                    "unique_customers"
                ).alias(
                    "max_customers"
                ),

                F.max(
                    "total_purchases"
                ).alias(
                    "max_purchases"
                ),

                F.max(
                    "total_views"
                ).alias(
                    "max_views"
                ),

                F.max(
                    "total_interaction_score"
                ).alias(
                    "max_interaction"
                ),
            )
            .first()
        )

        max_customers = float(
            stats["max_customers"] or 1
        )

        max_purchases = float(
            stats["max_purchases"] or 1
        )

        max_views = float(
            stats["max_views"] or 1
        )

        max_interaction = float(
            stats["max_interaction"] or 1
        )

        popularity = (
            popularity

            .withColumn(
                "customer_score",
                F.col(
                    "unique_customers"
                )
                /
                F.lit(
                    max_customers
                ),
            )

            .withColumn(
                "purchase_popularity",
                F.col(
                    "total_purchases"
                )
                /
                F.lit(
                    max_purchases
                ),
            )

            .withColumn(
                "view_popularity",
                F.col(
                    "total_views"
                )
                /
                F.lit(
                    max_views
                ),
            )

            .withColumn(
                "interaction_popularity",
                F.col(
                    "total_interaction_score"
                )
                /
                F.lit(
                    max_interaction
                ),
            )
        )

        # ====================================================
        # GLOBAL POPULARITY SCORE
        #
        # Baseline weights:
        #
        # customers    30%
        # purchases    35%
        # views        10%
        # interactions 25%
        #
        # These are initial engineering choices.
        # Later ML-2E will evaluate/tune the recommender.
        # ====================================================

        popularity = (
            popularity
            .withColumn(
                "popularity_score",

                F.col(
                    "customer_score"
                ) * F.lit(0.30)

                +

                F.col(
                    "purchase_popularity"
                ) * F.lit(0.35)

                +

                F.col(
                    "view_popularity"
                ) * F.lit(0.10)

                +

                F.col(
                    "interaction_popularity"
                ) * F.lit(0.25),
            )
        )

        # ====================================================
        # GLOBAL RANK
        # ====================================================

        global_window = (
            Window.orderBy(
                F.desc(
                    "popularity_score"
                ),
                F.desc(
                    "unique_customers"
                ),
                F.asc(
                    "product_id"
                ),
            )
        )

        popularity = (
            popularity
            .withColumn(
                "global_rank",
                F.row_number().over(
                    global_window
                ),
            )
        )

        # ====================================================
        # CATEGORY RANK
        # ====================================================

        category_window = (
            Window
            .partitionBy(
                "category"
            )
            .orderBy(
                F.desc(
                    "popularity_score"
                ),
                F.desc(
                    "unique_customers"
                ),
                F.asc(
                    "product_id"
                ),
            )
        )

        popularity = (
            popularity
            .withColumn(
                "category_rank",
                F.row_number().over(
                    category_window
                ),
            )
        )

        # ====================================================
        # SUBCATEGORY RANK
        # ====================================================

        subcategory_window = (
            Window
            .partitionBy(
                "category",
                "subcategory",
            )
            .orderBy(
                F.desc(
                    "popularity_score"
                ),
                F.desc(
                    "unique_customers"
                ),
                F.asc(
                    "product_id"
                ),
            )
        )

        popularity = (
            popularity
            .withColumn(
                "subcategory_rank",
                F.row_number().over(
                    subcategory_window
                ),
            )
        )

        # ====================================================
        # BRAND RANK
        # ====================================================

        brand_window = (
            Window
            .partitionBy(
                "brand"
            )
            .orderBy(
                F.desc(
                    "popularity_score"
                ),
                F.desc(
                    "unique_customers"
                ),
                F.asc(
                    "product_id"
                ),
            )
        )

        popularity = (
            popularity
            .withColumn(
                "brand_rank",
                F.row_number().over(
                    brand_window
                ),
            )
        )

        # ====================================================
        # METADATA
        # ====================================================

        popularity = (
            popularity
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

        # ====================================================
        # FINAL OUTPUT
        # ====================================================

        popularity = popularity.select(

            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "price",

            "unique_customers",
            "total_purchases",
            "total_units",
            "total_views",
            "total_interaction_score",

            "customer_score",
            "purchase_popularity",
            "view_popularity",
            "interaction_popularity",

            "popularity_score",

            "global_rank",
            "category_rank",
            "subcategory_rank",
            "brand_rank",

            "last_interaction_at",

            "model_name",
            "generated_at",
        )

        # ====================================================
        # VALIDATION
        # ====================================================

        product_count = popularity.count()

        print()
        print("=" * 70)
        print("POPULARITY STATISTICS")
        print("=" * 70)

        print(
            "Products:",
            f"{product_count:,}",
        )

        print()
        print("Top 20 products:")

        (
            popularity
            .orderBy(
                "global_rank"
            )
            .show(
                20,
                truncate=False,
            )
        )

        if product_count == 0:
            raise RuntimeError(
                "No popularity records generated."
            )

        invalid_scores = (
            popularity
            .filter(
                F.col(
                    "popularity_score"
                ).isNull()
                |
                (
                    F.col(
                        "popularity_score"
                    )
                    < 0
                )
                |
                (
                    F.col(
                        "popularity_score"
                    )
                    > 1
                )
            )
            .count()
        )

        if invalid_scores > 0:
            raise RuntimeError(
                f"{invalid_scores} invalid "
                "popularity scores found."
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
            popularity
            .writeTo(
                PRODUCT_POPULARITY_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print("=" * 70)
        print("SUCCESS")
        print("=" * 70)

        print(
            PRODUCT_POPULARITY_TABLE
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()