from __future__ import annotations

import json
import sys

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import create_iceberg_spark_session


METRICS_TABLE = (
    "retailpulse.ml.recommendation_validation_metrics"
)

CONFIG_TABLE = (
    "retailpulse.ml.recommendation_selected_config"
)

CONFIG_JSON = (
    "/opt/retailpulse/ml/artifacts/"
    "recommendation/"
    "selected_config.json"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Select Recommendation Model"
    )

    try:

        metrics = spark.table(
            METRICS_TABLE
        )

        best = (
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
            .limit(1)
        )

        row = best.first()

        if row is None:

            raise RuntimeError(
                "No validation metrics found."
            )

        print()
        print("=" * 70)
        print("SELECTED CONFIGURATION")
        print("=" * 70)

        print(
            "Model:",
            row["model"],
        )

        print(
            "Personalized weight:",
            row[
                "personalized_weight"
            ],
        )

        print(
            "Popularity weight:",
            row[
                "popularity_weight"
            ],
        )

        print(
            "Validation HitRate@10:",
            row[
                "hit_rate_at_10"
            ],
        )

        selected = (
            best
            .withColumn(
                "selected_by",
                F.lit(
                    "validation_hit_rate_at_10"
                ),
            )
            .withColumn(
                "selected_at",
                F.current_timestamp(),
            )
        )

        (
            selected
            .writeTo(
                CONFIG_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        config = {
            "model": row["model"],

            "personalized_weight": float(
                row[
                    "personalized_weight"
                ]
            ),

            "popularity_weight": float(
                row[
                    "popularity_weight"
                ]
            ),

            "top_k": 10,

            "selection_metric": (
                "validation_hit_rate_at_10"
            ),

            "validation_metrics": {
                "precision_at_10": float(
                    row[
                        "precision_at_10"
                    ]
                ),

                "recall_at_10": float(
                    row[
                        "recall_at_10"
                    ]
                ),

                "hit_rate_at_10": float(
                    row[
                        "hit_rate_at_10"
                    ]
                ),
            },
        }

        import os

        os.makedirs(
            os.path.dirname(
                CONFIG_JSON
            ),
            exist_ok=True,
        )

        with open(
            CONFIG_JSON,
            "w",
            encoding="utf-8",
        ) as f:

            json.dump(
                config,
                f,
                indent=2,
            )

        print()
        print(
            "Saved:",
            CONFIG_JSON,
        )

        print()
        print(
            "SUCCESS:",
            CONFIG_TABLE,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()