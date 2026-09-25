from __future__ import annotations

import os
import sys

from pyspark import StorageLevel
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
    RECOMMENDATION_EVALUATION_TABLE,
    RECOMMENDATION_EVAL_TOP_K,
)


OUTPUT_TABLE = (
    "retailpulse.ml.recommendation_eval_predictions"
)

PERSONALIZED_WEIGHT = 0.80
POPULARITY_WEIGHT = 0.20


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Evaluation Recommendations"
    )

    persisted = []

    def persist_on_disk(frame):
        # Reuse expensive branches without competing for execution heap.
        frame.persist(StorageLevel.DISK_ONLY)
        persisted.append(frame)
        return frame

    try:

        shuffle_partitions = int(
            os.getenv("RECOMMENDATION_EVAL_SHUFFLE_PARTITIONS", "200")
        )
        if shuffle_partitions < 1:
            raise ValueError("RECOMMENDATION_EVAL_SHUFFLE_PARTITIONS must be positive")
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Preserve smaller shuffle tasks on the shared local JVM.
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

        evaluation = spark.table(
            RECOMMENDATION_EVALUATION_TABLE
        )

        train = (
            evaluation
            .filter(
                F.col("split") == "TRAIN"
            )
            .select(
                "customer_id",
                "product_id",
                "interaction_score",
            )
        )

        train = persist_on_disk(train)

        test_customers = (
            evaluation
            .filter(
                F.col("split") == "TEST"
            )
            .select(
                "customer_id"
            )
            .distinct()
        )

        # ====================================================
        # POPULARITY BASELINE
        # ====================================================

        popularity = (
            train
            .groupBy("product_id")
            .agg(
                F.countDistinct(
                    "customer_id"
                ).alias(
                    "customer_count"
                )
            )
        )

        popularity = persist_on_disk(popularity)

        max_popularity = (
            popularity
            .agg(
                F.max(
                    "customer_count"
                ).alias("m")
            )
            .first()["m"]
        )

        if not max_popularity:
            raise RuntimeError(
                "Unable to calculate popularity."
            )

        popularity = (
            popularity
            .withColumn(
                "popularity_score",
                F.col(
                    "customer_count"
                ).cast("double")
                /
                F.lit(
                    float(max_popularity)
                ),
            )
        )

        # Keep a larger candidate pool than K.

        popular_candidates = (
            popularity
            .orderBy(
                F.desc("popularity_score"),
                F.asc("product_id"),
            )
            .limit(100)
        )

        # ====================================================
        # ITEM-ITEM SIMILARITY FROM TRAIN ONLY
        # ====================================================

        binary = (
            train
            .select(
                "customer_id",
                "product_id",
            )
            .distinct()
        )

        binary = persist_on_disk(binary)

        product_counts = (
            binary
            .groupBy("product_id")
            .agg(
                F.count(
                    "customer_id"
                ).alias(
                    "product_customers"
                )
            )
        )

        a = binary.alias("a")
        b = binary.alias("b")

        pairs = (
            a.join(
                b,
                (
                    F.col("a.customer_id")
                    ==
                    F.col("b.customer_id")
                )
                &
                (
                    F.col("a.product_id")
                    <
                    F.col("b.product_id")
                ),
                "inner",
            )
            .groupBy(
                F.col(
                    "a.product_id"
                ).alias("product_a"),
                F.col(
                    "b.product_id"
                ).alias("product_b"),
            )
            .agg(
                # Binary rows are unique, so each customer contributes once.
                F.count(
                    "a.customer_id"
                ).alias("co_count")
            )
        )

        count_a = (
            product_counts
            .select(
                F.col("product_id").alias(
                    "product_a"
                ),
                F.col("product_customers").alias(
                    "customers_a"
                ),
            )
        )

        count_b = (
            product_counts
            .select(
                F.col("product_id").alias(
                    "product_b"
                ),
                F.col("product_customers").alias(
                    "customers_b"
                ),
            )
        )

        pairs = (
            pairs
            .join(
                count_a,
                "product_a",
            )
            .join(
                count_b,
                "product_b",
            )
            .withColumn(
                "similarity",

                F.col("co_count").cast("double")
                /
                F.sqrt(
                    F.col("customers_a").cast("double")
                    *
                    F.col("customers_b").cast("double")
                ),
            )
        )

        pairs = persist_on_disk(pairs)

        forward = (
            pairs.select(
                F.col("product_a").alias(
                    "source_product"
                ),
                F.col("product_b").alias(
                    "candidate_product"
                ),
                "similarity",
            )
        )

        reverse = (
            pairs.select(
                F.col("product_b").alias(
                    "source_product"
                ),
                F.col("product_a").alias(
                    "candidate_product"
                ),
                "similarity",
            )
        )

        similarity = (
            forward.unionByName(reverse)
        )

        # ====================================================
        # PERSONALIZED CANDIDATES
        # ====================================================

        personalized = (
            train.join(test_customers, "customer_id", "left_semi").alias("i")
            .join(
                similarity.alias("s"),
                F.col("i.product_id")
                ==
                F.col("s.source_product"),
            )
            .select(
                F.col("i.customer_id").alias(
                    "customer_id"
                ),

                F.col("s.candidate_product").alias(
                    "product_id"
                ),

                (
                    F.col("i.interaction_score")
                    *
                    F.col("s.similarity")
                ).alias(
                    "contribution"
                ),
            )
            .groupBy(
                "customer_id",
                "product_id",
            )
            .agg(
                F.sum(
                    "contribution"
                ).alias(
                    "raw_personalized_score"
                )
            )
        )

        # Remove seen items.

        seen = (
            train
            .select(
                F.col("customer_id").alias(
                    "seen_customer"
                ),
                F.col("product_id").alias(
                    "seen_product"
                ),
            )
            .distinct()
        )

        personalized = (
            personalized
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

        # Normalize per customer.

        score_window = (
            Window.partitionBy(
                "customer_id"
            )
        )

        personalized = (
            personalized
            .withColumn(
                "max_score",
                F.max(
                    "raw_personalized_score"
                ).over(score_window),
            )
            .withColumn(
                "personalized_score",

                F.when(
                    F.col("max_score") > 0,

                    F.col(
                        "raw_personalized_score"
                    )
                    /
                    F.col("max_score"),

                ).otherwise(
                    F.lit(0.0)
                ),
            )
            .drop("max_score")
        )

        personalized = persist_on_disk(personalized)

        # ====================================================
        # POPULARITY RECOMMENDATIONS
        # ====================================================

        popularity_recs = (
            test_customers
            .crossJoin(
                popular_candidates
                .select(
                    "product_id",
                    "popularity_score",
                )
            )
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

        popularity_rank_window = (
            Window
            .partitionBy("customer_id")
            .orderBy(
                F.desc("popularity_score"),
                F.asc("product_id"),
            )
        )

        popularity_output = (
            popularity_recs
            .withColumn(
                "rank",
                F.row_number().over(
                    popularity_rank_window
                ),
            )
            .filter(
                F.col("rank")
                <= RECOMMENDATION_EVAL_TOP_K
            )
            .select(
                "customer_id",
                "product_id",
                F.col(
                    "popularity_score"
                ).alias("score"),
                "rank",
            )
            .withColumn(
                "model",
                F.lit("popularity"),
            )
        )

        # ====================================================
        # ITEM-ITEM RECOMMENDATIONS
        # ====================================================

        item_rank_window = (
            Window
            .partitionBy("customer_id")
            .orderBy(
                F.desc("personalized_score"),
                F.asc("product_id"),
            )
        )

        item_output = (
            personalized
            .withColumn(
                "rank",
                F.row_number().over(
                    item_rank_window
                ),
            )
            .filter(
                F.col("rank")
                <= RECOMMENDATION_EVAL_TOP_K
            )
            .select(
                "customer_id",
                "product_id",
                F.col(
                    "personalized_score"
                ).alias("score"),
                "rank",
            )
            .withColumn(
                "model",
                F.lit("item_item"),
            )
        )

        # ====================================================
        # HYBRID CANDIDATES
        # ====================================================

        hybrid = (
            personalized.alias("p")
            .join(
                popularity.select(
                    "product_id",
                    "popularity_score",
                ).alias("pop"),
                "product_id",
                "left",
            )
            .select(
                "customer_id",
                "product_id",
                "personalized_score",

                F.coalesce(
                    F.col("popularity_score"),
                    F.lit(0.0),
                ).alias(
                    "popularity_score"
                ),
            )
            .withColumn(
                "hybrid_score",

                F.col("personalized_score")
                * F.lit(PERSONALIZED_WEIGHT)

                +

                F.col("popularity_score")
                * F.lit(POPULARITY_WEIGHT),
            )
        )

        # Add popular fallback candidates so hybrid can still
        # fill Top-K.

        hybrid_popularity = (
            popularity_recs
            .select(
                "customer_id",
                "product_id",

                F.lit(0.0).alias(
                    "personalized_score"
                ),

                "popularity_score",
            )
            .withColumn(
                "hybrid_score",
                F.col("popularity_score"),
            )
        )

        hybrid = (
            hybrid
            .unionByName(
                hybrid_popularity
            )
            .groupBy(
                "customer_id",
                "product_id",
            )
            .agg(
                F.max(
                    "hybrid_score"
                ).alias(
                    "hybrid_score"
                )
            )
        )

        hybrid_rank_window = (
            Window
            .partitionBy("customer_id")
            .orderBy(
                F.desc("hybrid_score"),
                F.asc("product_id"),
            )
        )

        hybrid_output = (
            hybrid
            .withColumn(
                "rank",
                F.row_number().over(
                    hybrid_rank_window
                ),
            )
            .filter(
                F.col("rank")
                <= RECOMMENDATION_EVAL_TOP_K
            )
            .select(
                "customer_id",
                "product_id",
                F.col(
                    "hybrid_score"
                ).alias("score"),
                "rank",
            )
            .withColumn(
                "model",
                F.lit("hybrid_80_20"),
            )
        )

        # ====================================================
        # UNION
        # ====================================================

        output = (
            popularity_output
            .unionByName(item_output)
            .unionByName(hybrid_output)
            .withColumn(
                "generated_at",
                F.current_timestamp(),
            )
        )

        print()
        print("Evaluation recommendations:")

        output = persist_on_disk(output)

        (
            output
            .groupBy("model")
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct(
                    "customer_id"
                ).alias("customers"),
            )
            .show()
        )

        (
            output
            .writeTo(
                OUTPUT_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print(
            "SUCCESS:",
            OUTPUT_TABLE,
        )

    finally:
        try:
            for frame in reversed(persisted):
                frame.unpersist()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
