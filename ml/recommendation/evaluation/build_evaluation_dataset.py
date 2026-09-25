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


from iceberg_session import (
    create_iceberg_spark_session,
)

from common.ml_config import (
    RECOMMENDATION_INTERACTIONS_TABLE,
    RECOMMENDATION_EVALUATION_TABLE,
    RECOMMENDATION_TEST_DAYS,
    RECOMMENDATION_MIN_HISTORY,
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Recommendation Evaluation Dataset"
    )

    try:

        print("=" * 70)
        print("ML-2E RECOMMENDATION EVALUATION DATASET")
        print("=" * 70)

        interactions = (
            spark.table(
                RECOMMENDATION_INTERACTIONS_TABLE
            )
            .select(
                "customer_id",
                "product_id",
                "interaction_score",
                "last_interaction_at",
            )
            .filter(
                F.col("customer_id").isNotNull()
                &
                F.col("product_id").isNotNull()
                &
                F.col("last_interaction_at").isNotNull()
            )
        )

        # ----------------------------------------------------
        # Determine dataset end date
        # ----------------------------------------------------

        max_date = (
            interactions
            .agg(
                F.max(
                    "last_interaction_at"
                ).alias("max_date")
            )
            .first()["max_date"]
        )

        if max_date is None:
            raise RuntimeError(
                "No interaction timestamps found."
            )

        print(
            "Maximum interaction timestamp:",
            max_date,
        )

        # ----------------------------------------------------
        # Test cutoff
        #
        # Everything before cutoff = history
        # Everything on/after cutoff = future
        # ----------------------------------------------------

        cutoff_df = (
            spark.createDataFrame(
                [(max_date,)],
                ["max_date"],
            )
            .select(
                F.date_sub(
                    F.to_date("max_date"),
                    RECOMMENDATION_TEST_DAYS,
                ).alias("cutoff_date")
            )
        )

        cutoff_date = (
            cutoff_df.first()["cutoff_date"]
        )

        print(
            "Evaluation cutoff:",
            cutoff_date,
        )

        # ----------------------------------------------------
        # TRAIN HISTORY
        # ----------------------------------------------------

        history = (
            interactions
            .filter(
                F.to_date(
                    "last_interaction_at"
                )
                <
                F.lit(cutoff_date)
            )
        )

        # ----------------------------------------------------
        # FUTURE / TEST
        # ----------------------------------------------------

        future = (
            interactions
            .filter(
                F.to_date(
                    "last_interaction_at"
                )
                >=
                F.lit(cutoff_date)
            )
        )

        # ----------------------------------------------------
        # Eligible customers
        #
        # Need enough historical products and at least one
        # future item.
        # ----------------------------------------------------

        history_counts = (
            history
            .groupBy(
                "customer_id"
            )
            .agg(
                F.countDistinct(
                    "product_id"
                ).alias(
                    "history_product_count"
                )
            )
            .filter(
                F.col(
                    "history_product_count"
                )
                >=
                RECOMMENDATION_MIN_HISTORY
            )
        )

        future_counts = (
            future
            .groupBy(
                "customer_id"
            )
            .agg(
                F.countDistinct(
                    "product_id"
                ).alias(
                    "future_product_count"
                )
            )
            .filter(
                F.col(
                    "future_product_count"
                ) > 0
            )
        )

        eligible = (
            history_counts
            .join(
                future_counts,
                "customer_id",
                "inner",
            )
        )

        eligible_count = eligible.count()

        print(
            "Eligible customers:",
            f"{eligible_count:,}",
        )

        if eligible_count == 0:
            raise RuntimeError(
                "No eligible evaluation customers. "
                "The synthetic history may need a larger "
                "time range or a smaller test window."
            )

        # ----------------------------------------------------
        # Keep only eligible customers
        # ----------------------------------------------------

        history = (
            history
            .join(
                eligible.select(
                    "customer_id"
                ),
                "customer_id",
                "inner",
            )
        )

        future = (
            future
            .join(
                eligible.select(
                    "customer_id"
                ),
                "customer_id",
                "inner",
            )
        )

        # ----------------------------------------------------
        # Important:
        # Future products already present in history are not
        # useful for evaluating unseen-item recommendation.
        # ----------------------------------------------------

        seen = (
            history
            .select(
                F.col(
                    "customer_id"
                ).alias("seen_customer"),
                F.col(
                    "product_id"
                ).alias("seen_product"),
            )
            .distinct()
        )

        future_unseen = (
            future
            .join(
                seen,
                (
                    F.col("customer_id")
                    ==
                    F.col("seen_customer")
                )
                &
                (
                    F.col("product_id")
                    ==
                    F.col("seen_product")
                ),
                "left_anti",
            )
        )

        # ----------------------------------------------------
        # Build unified table
        # ----------------------------------------------------

        history_output = (
            history
            .select(
                "customer_id",
                "product_id",
                "interaction_score",
                "last_interaction_at",
            )
            .withColumn(
                "split",
                F.lit("TRAIN"),
            )
        )

        future_output = (
            future_unseen
            .select(
                "customer_id",
                "product_id",
                "interaction_score",
                "last_interaction_at",
            )
            .withColumn(
                "split",
                F.lit("TEST"),
            )
        )

        evaluation = (
            history_output
            .unionByName(
                future_output
            )
            .withColumn(
                "cutoff_date",
                F.lit(cutoff_date),
            )
            .withColumn(
                "generated_at",
                F.current_timestamp(),
            )
        )

        print()
        print("Split statistics:")

        (
            evaluation
            .groupBy("split")
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct(
                    "customer_id"
                ).alias("customers"),
                F.countDistinct(
                    "product_id"
                ).alias("products"),
            )
            .show()
        )

        test_count = (
            evaluation
            .filter(
                F.col("split") == "TEST"
            )
            .count()
        )

        if test_count == 0:
            raise RuntimeError(
                "No unseen future products remain "
                "for evaluation."
            )

        (
            evaluation
            .writeTo(
                RECOMMENDATION_EVALUATION_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "SUCCESS:",
            RECOMMENDATION_EVALUATION_TABLE,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()