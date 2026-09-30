from __future__ import annotations

import math
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import create_iceberg_spark_session


SPLIT_TABLE = (
    "retailpulse.ml.recommendation_temporal_split"
)

OUTPUT_TABLE = (
    "retailpulse.ml.recommendation_validation_metrics"
)

TOP_K = 10


HYBRID_WEIGHTS = [
    (1.00, 0.00, "item_item"),
    (0.90, 0.10, "hybrid_90_10"),
    (0.80, 0.20, "hybrid_80_20"),
    (0.70, 0.30, "hybrid_70_30"),
    (0.60, 0.40, "hybrid_60_40"),
    (0.50, 0.50, "hybrid_50_50"),
    (0.00, 1.00, "popularity"),
]


def build_recommendations(
    spark,
    train,
    target_customers,
):

    # ========================================================
    # Aggregate training interactions
    # ========================================================

    train_interactions = (
        train
        .groupBy(
            "customer_id",
            "product_id",
        )
        .agg(
            F.sum(
                "interaction_weight"
            ).alias(
                "interaction_score"
            )
        )
    )

    binary = (
        train_interactions
        .select(
            "customer_id",
            "product_id",
        )
        .distinct()
    )

    # ========================================================
    # Popularity
    # ========================================================

    popularity = (
        binary
        .groupBy("product_id")
        .agg(
            F.countDistinct(
                "customer_id"
            ).alias(
                "customer_count"
            )
        )
    )

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
            "No popularity data."
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

    # ========================================================
    # Product similarity
    # ========================================================

    product_counts = (
        binary
        .groupBy("product_id")
        .agg(
            F.countDistinct(
                "customer_id"
            ).alias(
                "customers"
            )
        )
    )

    a = binary.alias("a")
    b = binary.alias("b")

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
            F.countDistinct(
                "a.customer_id"
            ).alias(
                "co_count"
            )
        )
    )

    ca = (
        product_counts
        .select(
            F.col(
                "product_id"
            ).alias("product_a"),

            F.col(
                "customers"
            ).alias("customers_a"),
        )
    )

    cb = (
        product_counts
        .select(
            F.col(
                "product_id"
            ).alias("product_b"),

            F.col(
                "customers"
            ).alias("customers_b"),
        )
    )

    pairs = (
        pairs
        .join(
            ca,
            "product_a",
        )
        .join(
            cb,
            "product_b",
        )
        .withColumn(
            "similarity",

            F.col(
                "co_count"
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

    forward = (
        pairs
        .select(
            F.col(
                "product_a"
            ).alias(
                "source_product"
            ),

            F.col(
                "product_b"
            ).alias(
                "candidate_product"
            ),

            "similarity",
        )
    )

    reverse = (
        pairs
        .select(
            F.col(
                "product_b"
            ).alias(
                "source_product"
            ),

            F.col(
                "product_a"
            ).alias(
                "candidate_product"
            ),

            "similarity",
        )
    )

    similarity = (
        forward.unionByName(
            reverse
        )
    )

    # ========================================================
    # Personalized candidates
    # ========================================================

    personalized = (
        train_interactions.alias("i")
        .join(
            similarity.alias("s"),

            F.col(
                "i.product_id"
            )
            ==
            F.col(
                "s.source_product"
            ),
        )
        .select(
            F.col(
                "i.customer_id"
            ).alias(
                "customer_id"
            ),

            F.col(
                "s.candidate_product"
            ).alias(
                "product_id"
            ),

            (
                F.col(
                    "i.interaction_score"
                )
                *
                F.col(
                    "s.similarity"
                )
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

    # ========================================================
    # Remove seen products
    # ========================================================

    seen = (
        binary
        .select(
            F.col(
                "customer_id"
            ).alias(
                "seen_customer"
            ),

            F.col(
                "product_id"
            ).alias(
                "seen_product"
            ),
        )
    )

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
                    "seen_customer"
                )
            )
            &
            (
                F.col(
                    "product_id"
                )
                ==
                F.col(
                    "seen_product"
                )
            ),

            "left_anti",
        )
    )

    # ========================================================
    # Normalize personalized score
    # ========================================================

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
            ).over(
                score_window
            ),
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

    # ========================================================
    # Popularity candidates
    # ========================================================

    popular_candidates = (
        target_customers
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
                    "seen_customer"
                )
            )
            &
            (
                F.col(
                    "product_id"
                )
                ==
                F.col(
                    "seen_product"
                )
            ),

            "left_anti",
        )
        .select(
            "customer_id",
            "product_id",
            "popularity_score",
        )
    )

    # ========================================================
    # Candidate union
    # ========================================================

    personalized = (
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
        )
    )

    fallback = (
        popular_candidates
        .withColumn(
            "personalized_score",
            F.lit(0.0),
        )
        .select(
            "customer_id",
            "product_id",
            "personalized_score",
            "popularity_score",
        )
    )

    candidates = (
        personalized
        .unionByName(
            fallback
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
        )
    )

    return candidates


def calculate_metrics(
    recommendations,
    actual,
    model_name,
    personalized_weight,
    popularity_weight,
):

    scored = (
        recommendations.alias("r")
        .join(
            actual.alias("a"),

            (
                F.col(
                    "r.customer_id"
                )
                ==
                F.col(
                    "a.customer_id"
                )
            )
            &
            (
                F.col(
                    "r.product_id"
                )
                ==
                F.col(
                    "a.product_id"
                )
            ),

            "left",
        )
        # Keep only recommendation keys; the explicit join retains both sides.
        .select(
            "r.*",
            F.when(
                F.col(
                    "a.product_id"
                ).isNotNull(),

                F.lit(1.0),

            ).otherwise(
                F.lit(0.0)
            ).alias("hit"),
        )
    )

    actual_counts = (
        actual
        .groupBy(
            "customer_id"
        )
        .agg(
            F.countDistinct(
                "product_id"
            ).alias(
                "actual_count"
            )
        )
    )

    per_customer = (
        scored
        .groupBy(
            "customer_id"
        )
        .agg(
            F.sum(
                "hit"
            ).alias("hits"),

            F.count("*").alias(
                "recommended_count"
            ),
        )
        .join(
            actual_counts,
            "customer_id",
        )
        .withColumn(
            "precision",

            F.col("hits")
            /
            F.col(
                "recommended_count"
            ),
        )
        .withColumn(
            "recall",

            F.col("hits")
            /
            F.col(
                "actual_count"
            ),
        )
        .withColumn(
            "hit_rate",

            F.when(
                F.col("hits") > 0,
                F.lit(1.0),
            ).otherwise(
                F.lit(0.0)
            ),
        )
    )

    result = (
        per_customer
        .agg(
            F.avg(
                "precision"
            ).alias(
                "precision_at_10"
            ),

            F.avg(
                "recall"
            ).alias(
                "recall_at_10"
            ),

            F.avg(
                "hit_rate"
            ).alias(
                "hit_rate_at_10"
            ),

            F.count("*").alias(
                "evaluated_customers"
            ),
        )
        .withColumn(
            "model",
            F.lit(model_name),
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
    )

    return result


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Hybrid Weight Tuning"
    )

    persisted = []

    def persist_on_disk(frame):
        frame.persist(StorageLevel.DISK_ONLY)
        persisted.append(frame)
        return frame

    try:

        shuffle_partitions = int(
            os.getenv("RECOMMENDATION_TUNING_SHUFFLE_PARTITIONS", "200")
        )
        if shuffle_partitions < 1:
            raise ValueError("RECOMMENDATION_TUNING_SHUFFLE_PARTITIONS must be positive")
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

        print()
        print("=" * 70)
        print("HYBRID WEIGHT TUNING")
        print("=" * 70)

        split = spark.table(
            SPLIT_TABLE
        )

        train = (
            split
            .filter(
                F.col("split") == "TRAIN"
            )
        )

        validation = (
            split
            .filter(
                F.col("split")
                ==
                "VALIDATION"
            )
        )

        # Only customers with both history and
        # validation behavior are meaningful here.

        train_customers = (
            train
            .select(
                "customer_id"
            )
            .distinct()
        )

        validation_actual = (
            validation
            .select(
                "customer_id",
                "product_id",
            )
            .distinct()
            .join(
                train_customers,
                "customer_id",
                "inner",
            )
        )

        target_customers = (
            validation_actual
            .select(
                "customer_id"
            )
            .distinct()
        )

        # Remove validation items already seen
        # during training.

        train_seen = (
            train
            .select(
                F.col(
                    "customer_id"
                ).alias(
                    "seen_customer"
                ),

                F.col(
                    "product_id"
                ).alias(
                    "seen_product"
                ),
            )
            .distinct()
        )

        validation_actual = (
            validation_actual
            .join(
                train_seen,

                (
                    F.col(
                        "customer_id"
                    )
                    ==
                    F.col(
                        "seen_customer"
                    )
                )
                &
                (
                    F.col(
                        "product_id"
                    )
                    ==
                    F.col(
                        "seen_product"
                    )
                ),

                "left_anti",
            )
        )

        # Customers need at least one unseen
        # validation product.

        target_customers = (
            validation_actual
            .select(
                "customer_id"
            )
            .distinct()
        )

        validation_actual = persist_on_disk(validation_actual)
        target_customers = persist_on_disk(target_customers)

        target_count = (
            target_customers.count()
        )

        print(
            "Validation customers:",
            f"{target_count:,}",
        )

        if target_count == 0:

            raise RuntimeError(
                "No eligible validation customers."
            )

        candidates = (
            build_recommendations(
                spark,
                train,
                target_customers,
            )
        )

        candidates = persist_on_disk(candidates)
        print("Materializing candidate scores on disk...")
        print("Candidate rows:", f"{candidates.count():,}")

        results = []
        result_schema = None

        for (
            personalized_weight,
            popularity_weight,
            model_name,
        ) in HYBRID_WEIGHTS:

            print(
                "Evaluating:",
                model_name,
            )

            ranked = (
                candidates
                .withColumn(
                    "score",

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

            rank_window = (
                Window
                .partitionBy(
                    "customer_id"
                )
                .orderBy(
                    F.desc("score"),
                    F.asc("product_id"),
                )
            )

            recommendations = (
                ranked
                .withColumn(
                    "rank",

                    F.row_number().over(
                        rank_window
                    ),
                )
                .filter(
                    F.col("rank")
                    <=
                    TOP_K
                )
            )

            result = calculate_metrics(
                recommendations,
                validation_actual,
                model_name,
                personalized_weight,
                popularity_weight,
            )

            # Each aggregate has one row. Finish this weight before starting
            # the next; do not union seven expensive lazy query branches.
            result_schema = result.schema
            results.extend(result.collect())

        # Preserve the schema even when an aggregate metric is null. Neither
        # show() nor the Iceberg write can now rerun candidate generation.
        metrics = spark.createDataFrame(results, schema=result_schema)

        metrics = (
            metrics
            .withColumn(
                "selection_metric",
                F.lit(
                    "hit_rate_at_10"
                ),
            )
            .withColumn(
                "evaluated_at",
                F.current_timestamp(),
            )
        )

        print()
        print("VALIDATION RESULTS")

        (
            metrics
            .orderBy(
                F.desc(
                    "hit_rate_at_10"
                ),
                F.desc(
                    "recall_at_10"
                ),
                F.desc(
                    "precision_at_10"
                ),
            )
            .show(
                truncate=False
            )
        )

        (
            metrics
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
        try:
            for frame in reversed(persisted):
                frame.unpersist()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
