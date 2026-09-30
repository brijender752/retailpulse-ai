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


from iceberg_session import create_iceberg_spark_session


SPLIT_TABLE = (
    "retailpulse.ml.recommendation_temporal_split"
)

CONFIG_TABLE = (
    "retailpulse.ml.recommendation_selected_config"
)

PREDICTIONS_TABLE = (
    "retailpulse.ml.recommendation_final_test_predictions"
)

METRICS_TABLE = (
    "retailpulse.ml.recommendation_final_test_metrics"
)

PRODUCTS_TABLE = (
    "retailpulse.silver.products"
)

TOP_K = 10


# ============================================================
# BUILD CANDIDATES
# ============================================================

def build_candidates(
    train,
    target_customers,
    persist_frame=None,
):

    if persist_frame is None:
        persist_frame = lambda frame: frame

    # --------------------------------------------------------
    # Aggregate TRAIN + VALIDATION events
    # --------------------------------------------------------

    interactions = (
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
        .filter(
            F.col(
                "interaction_score"
            ) > 0
        )
    )

    interactions = persist_frame(interactions)

    binary = (
        interactions
        .select(
            "customer_id",
            "product_id",
        )
        .distinct()
    )

    # ========================================================
    # POPULARITY
    # ========================================================

    popularity = (
        binary
        .groupBy(
            "product_id"
        )
        .agg(
            F.countDistinct(
                "customer_id"
            ).alias(
                "customer_count"
            )
        )
    )

    popularity = persist_frame(popularity)

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
            "Unable to calculate product popularity."
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
                float(
                    max_popularity
                )
            ),
        )
    )

    # ========================================================
    # ITEM-ITEM SIMILARITY
    # ========================================================

    product_counts = (
        binary
        .groupBy(
            "product_id"
        )
        .agg(
            F.countDistinct(
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
        .groupBy(
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
        .agg(
            F.countDistinct(
                "a.customer_id"
            ).alias(
                "co_count"
            )
        )
    )

    count_a = (
        product_counts
        .select(
            F.col(
                "product_id"
            ).alias(
                "product_a"
            ),

            F.col(
                "product_customers"
            ).alias(
                "customers_a"
            ),
        )
    )

    count_b = (
        product_counts
        .select(
            F.col(
                "product_id"
            ).alias(
                "product_b"
            ),

            F.col(
                "product_customers"
            ).alias(
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
        .filter(
            F.col(
                "similarity"
            ) > 0
        )
    )

    pairs = persist_frame(pairs)

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
        forward
        .unionByName(
            reverse
        )
    )

    # ========================================================
    # PERSONALIZED CANDIDATES
    # ========================================================

    personalized = (
        interactions.alias("i")
        .join(
            similarity.alias("s"),

            F.col(
                "i.product_id"
            )
            ==
            F.col(
                "s.source_product"
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
    # REMOVE PRODUCTS ALREADY SEEN
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
        .distinct()
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
    # NORMALIZE PERSONALIZED SCORE
    # ========================================================

    score_window = (
        Window.partitionBy(
            "customer_id"
        )
    )

    personalized = (
        personalized
        .withColumn(
            "max_personalized_score",

            F.max(
                "raw_personalized_score"
            ).over(
                score_window
            ),
        )
        .withColumn(
            "personalized_score",

            F.when(
                F.col(
                    "max_personalized_score"
                ) > 0,

                F.col(
                    "raw_personalized_score"
                )
                /
                F.col(
                    "max_personalized_score"
                ),

            ).otherwise(
                F.lit(0.0)
            ),
        )
        .drop(
            "max_personalized_score"
        )
    )

    # ========================================================
    # POPULARITY FALLBACK
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
    # ADD POPULARITY TO PERSONALIZED CANDIDATES
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

    # ========================================================
    # UNION
    # ========================================================

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


# ============================================================
# MAIN
# ============================================================

def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Final Recommendation Test"
    )

    persisted = []

    def persist_on_disk(frame):
        frame.persist(StorageLevel.DISK_ONLY)
        persisted.append(frame)
        return frame

    try:
        partitions = int(os.getenv("RECOMMENDATION_FINAL_TEST_SHUFFLE_PARTITIONS", "200"))
        if partitions < 1:
            raise ValueError("RECOMMENDATION_FINAL_TEST_SHUFFLE_PARTITIONS must be positive")
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

        print()
        print("=" * 70)
        print("ML-2G FINAL TEST EVALUATION")
        print("=" * 70)

        split = spark.table(
            SPLIT_TABLE
        )

        # ====================================================
        # READ CONFIG SELECTED FROM VALIDATION
        # ====================================================

        config = (
            spark.table(
                CONFIG_TABLE
            )
            .first()
        )

        if config is None:

            raise RuntimeError(
                "No selected recommendation "
                "configuration found."
            )

        model_name = (
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
        print(
            "Selected model:",
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

        print()
        print(
            "This configuration was selected "
            "using VALIDATION only."
        )

        # ====================================================
        # TRAIN + VALIDATION
        # ====================================================

        train_validation = (
            split
            .filter(
                F.col("split").isin(
                    "TRAIN",
                    "VALIDATION",
                )
            )
        )

        test = (
            split
            .filter(
                F.col(
                    "split"
                ) == "TEST"
            )
        )

        # ====================================================
        # TEST ACTUAL PRODUCTS
        # ====================================================

        history_customers = (
            train_validation
            .select(
                "customer_id"
            )
            .distinct()
        )

        actual = (
            test
            .select(
                "customer_id",
                "product_id",
            )
            .distinct()
            .join(
                history_customers,
                "customer_id",
                "inner",
            )
        )

        # ----------------------------------------------------
        # We evaluate unseen-item recommendation.
        # Remove TEST items already seen historically.
        # ----------------------------------------------------

        history_seen = (
            train_validation
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

        actual = (
            actual
            .join(
                history_seen,

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

        actual = persist_on_disk(actual)

        target_customers = (
            actual
            .select(
                "customer_id"
            )
            .distinct()
        )

        target_customers = persist_on_disk(target_customers)

        target_count = (
            target_customers
            .count()
        )

        actual_count = (
            actual.count()
        )

        print()
        print(
            "Eligible TEST customers:",
            f"{target_count:,}",
        )

        print(
            "Unseen TEST interactions:",
            f"{actual_count:,}",
        )

        if target_count == 0:

            raise RuntimeError(
                "No eligible TEST customers "
                "with unseen future products."
            )

        # ====================================================
        # BUILD FINAL CANDIDATES USING TRAIN + VALIDATION
        # ====================================================

        candidates = (
            build_candidates(
                train_validation,
                target_customers,
                persist_frame=persist_on_disk,
            )
        )

        # ====================================================
        # APPLY ONLY SELECTED WEIGHTS
        # ====================================================

        scored_candidates = (
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

        predictions = (
            scored_candidates
            .withColumn(
                "rank",

                F.row_number().over(
                    ranking_window
                ),
            )
            .filter(
                F.col("rank")
                <= TOP_K
            )
            .withColumn(
                "model",
                F.lit(
                    str(model_name)
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

        predictions = persist_on_disk(predictions)

        prediction_count = (
            predictions.count()
        )

        if prediction_count == 0:

            raise RuntimeError(
                "Final recommender produced "
                "zero predictions."
            )

        # ====================================================
        # SCORE RECOMMENDATIONS
        # ====================================================

        scored = (
            predictions.alias("r")
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
            .select(
                "r.*",

                F.when(
                    F.col(
                        "a.product_id"
                    ).isNotNull(),

                    F.lit(1.0),

                ).otherwise(
                    F.lit(0.0)
                ).alias(
                    "hit"
                ),
            )
        )

        scored = persist_on_disk(scored)

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

        # ====================================================
        # PRECISION / RECALL / HIT RATE
        # ====================================================

        customer_basic = (
            scored
            .groupBy(
                "customer_id"
            )
            .agg(
                F.sum(
                    "hit"
                ).alias(
                    "hits"
                ),

                F.count(
                    "*"
                ).alias(
                    "recommended_count"
                ),
            )
            .join(
                actual_counts,
                "customer_id",
                "inner",
            )
            .withColumn(
                "precision_at_10",

                F.col("hits")
                /
                F.col(
                    "recommended_count"
                ),
            )
            .withColumn(
                "recall_at_10",

                F.col("hits")
                /
                F.col(
                    "actual_count"
                ),
            )
            .withColumn(
                "hit_rate_at_10",

                F.when(
                    F.col("hits") > 0,
                    F.lit(1.0),
                ).otherwise(
                    F.lit(0.0)
                ),
            )
        )

        # ====================================================
        # MAP@10
        # ====================================================

        cumulative_window = (
            Window
            .partitionBy(
                "customer_id"
            )
            .orderBy(
                "rank"
            )
            .rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow,
            )
        )

        ap_rows = (
            scored
            .withColumn(
                "cumulative_hits",

                F.sum(
                    "hit"
                ).over(
                    cumulative_window
                ),
            )
            .withColumn(
                "precision_at_rank",

                F.col(
                    "cumulative_hits"
                )
                /
                F.col(
                    "rank"
                ),
            )
            .withColumn(
                "ap_component",

                F.when(
                    F.col("hit") == 1,

                    F.col(
                        "precision_at_rank"
                    ),

                ).otherwise(
                    F.lit(0.0)
                ),
            )
        )

        average_precision = (
            ap_rows
            .groupBy(
                "customer_id"
            )
            .agg(
                F.sum(
                    "ap_component"
                ).alias(
                    "ap_sum"
                )
            )
            .join(
                actual_counts,
                "customer_id",
            )
            .withColumn(
                "map_customer",

                F.col(
                    "ap_sum"
                )
                /
                F.least(
                    F.col(
                        "actual_count"
                    ),
                    F.lit(
                        TOP_K
                    ),
                ),
            )
            .select(
                "customer_id",
                "map_customer",
            )
        )

        # ====================================================
        # NDCG@10
        # ====================================================

        dcg = (
            scored
            .withColumn(
                "dcg_component",

                F.col("hit")
                /
                (
                    F.log(
                        F.col("rank")
                        +
                        F.lit(1.0)
                    )
                    /
                    F.log(
                        F.lit(2.0)
                    )
                ),
            )
            .groupBy(
                "customer_id"
            )
            .agg(
                F.sum(
                    "dcg_component"
                ).alias(
                    "dcg"
                )
            )
        )

        positions = (
            spark.range(
                1,
                TOP_K + 1,
            )
            .withColumnRenamed(
                "id",
                "position",
            )
        )

        idcg = (
            actual_counts
            .withColumn(
                "ideal_hits",

                F.least(
                    F.col(
                        "actual_count"
                    ),
                    F.lit(
                        TOP_K
                    ),
                ),
            )
            .crossJoin(
                positions
            )
            .filter(
                F.col(
                    "position"
                )
                <=
                F.col(
                    "ideal_hits"
                )
            )
            .withColumn(
                "ideal_gain",

                F.lit(1.0)
                /
                (
                    F.log(
                        F.col(
                            "position"
                        )
                        +
                        F.lit(1.0)
                    )
                    /
                    F.log(
                        F.lit(2.0)
                    )
                ),
            )
            .groupBy(
                "customer_id"
            )
            .agg(
                F.sum(
                    "ideal_gain"
                ).alias(
                    "idcg"
                )
            )
        )

        ndcg = (
            dcg
            .join(
                idcg,
                "customer_id",
            )
            .withColumn(
                "ndcg_customer",

                F.when(
                    F.col(
                        "idcg"
                    ) > 0,

                    F.col(
                        "dcg"
                    )
                    /
                    F.col(
                        "idcg"
                    ),

                ).otherwise(
                    F.lit(0.0)
                ),
            )
            .select(
                "customer_id",
                "ndcg_customer",
            )
        )

        # ====================================================
        # CUSTOMER-LEVEL METRICS
        # ====================================================

        customer_metrics = (
            customer_basic
            .join(
                average_precision,
                "customer_id",
                "left",
            )
            .join(
                ndcg,
                "customer_id",
                "left",
            )
        )

        # ====================================================
        # CATALOG COVERAGE
        # ====================================================

        catalog_count = (
            spark.table(
                PRODUCTS_TABLE
            )
            .select(
                "product_id"
            )
            .distinct()
            .count()
        )

        recommended_products = (
            predictions
            .select(
                "product_id"
            )
            .distinct()
            .count()
        )

        if catalog_count == 0:

            raise RuntimeError(
                "Product catalog is empty."
            )

        catalog_coverage = (
            recommended_products
            /
            catalog_count
        )

        # ====================================================
        # FINAL TEST METRICS
        # ====================================================

        metrics = (
            customer_metrics
            .agg(
                F.avg(
                    "precision_at_10"
                ).alias(
                    "precision_at_10"
                ),

                F.avg(
                    "recall_at_10"
                ).alias(
                    "recall_at_10"
                ),

                F.avg(
                    "hit_rate_at_10"
                ).alias(
                    "hit_rate_at_10"
                ),

                F.avg(
                    "map_customer"
                ).alias(
                    "map_at_10"
                ),

                F.avg(
                    "ndcg_customer"
                ).alias(
                    "ndcg_at_10"
                ),

                F.count(
                    "*"
                ).alias(
                    "evaluated_customers"
                ),
            )
            .withColumn(
                "model",
                F.lit(
                    str(model_name)
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
                "recommended_products",
                F.lit(
                    recommended_products
                ),
            )
            .withColumn(
                "catalog_size",
                F.lit(
                    catalog_count
                ),
            )
            .withColumn(
                "catalog_coverage",
                F.lit(
                    float(
                        catalog_coverage
                    )
                ),
            )
            .withColumn(
                "evaluation_dataset",
                F.lit(
                    "TEST"
                ),
            )
            .withColumn(
                "top_k",
                F.lit(
                    TOP_K
                ),
            )
            .withColumn(
                "evaluated_at",
                F.current_timestamp(),
            )
        )

        print()
        print("=" * 70)
        print("FINAL UNTOUCHED TEST RESULTS")
        print("=" * 70)

        # Only one aggregate row reaches the driver. Detach its lineage so
        # displaying and writing metrics cannot repeat the evaluation joins.
        metrics = spark.createDataFrame(metrics.collect(), schema=metrics.schema)

        metrics.show(
            truncate=False
        )

        # ====================================================
        # WRITE PREDICTIONS
        # ====================================================

        (
            predictions
            .writeTo(
                PREDICTIONS_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        # ====================================================
        # WRITE METRICS
        # ====================================================

        (
            metrics
            .writeTo(
                METRICS_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "Predictions:",
            PREDICTIONS_TABLE,
        )

        print(
            "Metrics:",
            METRICS_TABLE,
        )

        print()
        print("=" * 70)
        print("FINAL TEST COMPLETE")
        print("=" * 70)

        print(
            "No alternative model was evaluated "
            "on TEST."
        )

    finally:
        try:
            for frame in reversed(persisted):
                frame.unpersist()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
