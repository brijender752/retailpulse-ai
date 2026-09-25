from __future__ import annotations

import sys

from pyspark.sql import functions as F
from pyspark.sql import Window


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
    RECOMMENDATION_METRICS_TABLE,
    RECOMMENDATION_EVAL_TOP_K,
)


PREDICTIONS_TABLE = (
    "retailpulse.ml.recommendation_eval_predictions"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Recommendation Metrics"
    )

    try:

        predictions = (
            spark.table(
                PREDICTIONS_TABLE
            )
        )

        actual = (
            spark.table(
                RECOMMENDATION_EVALUATION_TABLE
            )
            .filter(
                F.col("split") == "TEST"
            )
            .select(
                "customer_id",
                F.col("product_id").alias(
                    "actual_product_id"
                ),
            )
            .distinct()
        )

        actual_counts = (
            actual
            .groupBy("customer_id")
            .agg(
                F.count("*").alias(
                    "actual_count"
                )
            )
        )

        # ====================================================
        # HIT PER RECOMMENDATION
        # ====================================================

        scored = (
            predictions.alias("p")
            .join(
                actual.alias("a"),
                (
                    F.col("p.customer_id")
                    ==
                    F.col("a.customer_id")
                )
                &
                (
                    F.col("p.product_id")
                    ==
                    F.col("a.actual_product_id")
                ),
                "left",
            )
            # Keep one customer_id for every downstream metric and window.
            .select(
                "p.*",
                F.when(
                    F.col(
                        "a.actual_product_id"
                    ).isNotNull(),
                    F.lit(1.0),
                ).otherwise(
                    F.lit(0.0),
                ).alias("hit"),
            )
        )

        # ====================================================
        # PER CUSTOMER BASIC METRICS
        # ====================================================

        customer_metrics = (
            scored
            .groupBy(
                "model",
                "customer_id",
            )
            .agg(
                F.sum("hit").alias("hits"),
                F.count("*").alias(
                    "recommended_count"
                ),
            )
            .join(
                actual_counts,
                "customer_id",
                "inner",
            )
            .withColumn(
                "precision_at_k",
                F.col("hits")
                /
                F.col("recommended_count"),
            )
            .withColumn(
                "recall_at_k",
                F.col("hits")
                /
                F.col("actual_count"),
            )
            .withColumn(
                "hit_rate_at_k",
                F.when(
                    F.col("hits") > 0,
                    F.lit(1.0),
                ).otherwise(
                    F.lit(0.0),
                ),
            )
        )

        # ====================================================
        # NDCG
        #
        # DCG = hit / log2(rank + 1)
        # ====================================================

        ranked = (
            scored
            .withColumn(
                "dcg_component",

                F.col("hit")
                /
                (
                    F.log(
                        F.col("rank") + F.lit(1.0)
                    )
                    /
                    F.log(F.lit(2.0))
                ),
            )
        )

        dcg = (
            ranked
            .groupBy(
                "model",
                "customer_id",
            )
            .agg(
                F.sum(
                    "dcg_component"
                ).alias("dcg")
            )
        )

        # Ideal number of hits cannot exceed K.

        idcg_base = (
            actual_counts
            .withColumn(
                "ideal_hits",
                F.least(
                    F.col("actual_count"),
                    F.lit(
                        RECOMMENDATION_EVAL_TOP_K
                    ),
                ),
            )
        )

        positions = (
            spark.range(
                1,
                RECOMMENDATION_EVAL_TOP_K + 1,
            )
            .withColumnRenamed(
                "id",
                "position",
            )
        )

        idcg = (
            idcg_base
            .crossJoin(positions)
            .filter(
                F.col("position")
                <=
                F.col("ideal_hits")
            )
            .withColumn(
                "gain",

                F.lit(1.0)
                /
                (
                    F.log(
                        F.col("position")
                        +
                        F.lit(1.0)
                    )
                    /
                    F.log(F.lit(2.0))
                ),
            )
            .groupBy(
                "customer_id"
            )
            .agg(
                F.sum("gain").alias(
                    "idcg"
                )
            )
        )

        customer_metrics = (
            customer_metrics
            .join(
                dcg,
                [
                    "model",
                    "customer_id",
                ],
                "left",
            )
            .join(
                idcg,
                "customer_id",
                "left",
            )
            .withColumn(
                "ndcg_at_k",

                F.when(
                    F.col("idcg") > 0,
                    F.col("dcg")
                    /
                    F.col("idcg"),
                ).otherwise(
                    F.lit(0.0)
                ),
            )
        )

        # ====================================================
        # MAP@K
        # ====================================================

        rank_window = (
            Window
            .partitionBy(
                "model",
                "customer_id",
            )
            .orderBy("rank")
            .rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow,
            )
        )

        ap_rows = (
            scored
            .withColumn(
                "cumulative_hits",
                F.sum("hit").over(
                    rank_window
                ),
            )
            .withColumn(
                "precision_at_rank",

                F.col("cumulative_hits")
                /
                F.col("rank"),
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

        ap = (
            ap_rows
            .groupBy(
                "model",
                "customer_id",
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
                "ap_at_k",

                F.col("ap_sum")
                /
                F.least(
                    F.col("actual_count"),
                    F.lit(
                        RECOMMENDATION_EVAL_TOP_K
                    ),
                ),
            )
            .select(
                "model",
                "customer_id",
                "ap_at_k",
            )
        )

        customer_metrics = (
            customer_metrics
            .join(
                ap,
                [
                    "model",
                    "customer_id",
                ],
                "left",
            )
        )

        # ====================================================
        # AGGREGATE MODEL METRICS
        # ====================================================

        metrics = (
            customer_metrics
            .groupBy("model")
            .agg(
                F.countDistinct(
                    "customer_id"
                ).alias(
                    "evaluated_customers"
                ),

                F.avg(
                    "precision_at_k"
                ).alias(
                    "precision_at_10"
                ),

                F.avg(
                    "recall_at_k"
                ).alias(
                    "recall_at_10"
                ),

                F.avg(
                    "hit_rate_at_k"
                ).alias(
                    "hit_rate_at_10"
                ),

                F.avg(
                    "ap_at_k"
                ).alias(
                    "map_at_10"
                ),

                F.avg(
                    "ndcg_at_k"
                ).alias(
                    "ndcg_at_10"
                ),
            )
        )

        # ====================================================
        # CATALOG COVERAGE
        # ====================================================

        total_catalog = (
            spark.table(
                "retailpulse.silver.products"
            )
            .select("product_id")
            .distinct()
            .count()
        )

        coverage = (
            predictions
            .groupBy("model")
            .agg(
                F.countDistinct(
                    "product_id"
                ).alias(
                    "recommended_products"
                )
            )
            .withColumn(
                "catalog_coverage",

                F.col(
                    "recommended_products"
                )
                /
                F.lit(
                    float(total_catalog)
                ),
            )
        )

        metrics = (
            metrics
            .join(
                coverage,
                "model",
            )
            .withColumn(
                "k",
                F.lit(
                    RECOMMENDATION_EVAL_TOP_K
                ),
            )
            .withColumn(
                "evaluated_at",
                F.current_timestamp(),
            )
        )

        print()
        print("=" * 70)
        print("RECOMMENDATION EVALUATION")
        print("=" * 70)

        metrics.orderBy(
            F.desc("ndcg_at_10")
        ).show(
            truncate=False
        )

        (
            metrics
            .writeTo(
                RECOMMENDATION_METRICS_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "SUCCESS:",
            RECOMMENDATION_METRICS_TABLE,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
