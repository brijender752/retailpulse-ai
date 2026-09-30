from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import pandas as pd

from minio import Minio


MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow:5000",
)

EXPERIMENT_NAME = os.getenv(
    "RECOMMENDATION_MLFLOW_EXPERIMENT",
    "retailpulse-recommendation",
)


MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    os.getenv(
        "AWS_ACCESS_KEY_ID",
        "minioadmin",
    ),
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    os.getenv(
        "AWS_SECRET_ACCESS_KEY",
        "minioadmin",
    ),
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retailpulse",
)


METRICS_PREFIX = (
    "ml/exports/"
    "recommendation_final_test_metrics/"
)


DOWNLOAD_DIR = Path(
    "/tmp/recommendation_final_metrics"
)

ARTIFACT_DIR = Path(
    "/opt/retailpulse/ml/"
    "artifacts/recommendation"
)


def download_metrics():

    if DOWNLOAD_DIR.exists():

        shutil.rmtree(
            DOWNLOAD_DIR
        )

    DOWNLOAD_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    objects = list(
        client.list_objects(
            MINIO_BUCKET,
            prefix=METRICS_PREFIX,
            recursive=True,
        )
    )

    parquet_objects = [
        obj
        for obj in objects
        if obj.object_name.endswith(
            ".parquet"
        )
    ]

    if not parquet_objects:

        raise RuntimeError(
            "No final recommendation TEST metrics found."
        )

    files = []

    for index, obj in enumerate(
        parquet_objects
    ):

        file = (
            DOWNLOAD_DIR
            /
            f"part-{index:05d}.parquet"
        )

        client.fget_object(
            MINIO_BUCKET,
            obj.object_name,
            str(file),
        )

        files.append(
            file
        )

    df = pd.concat(
        [
            pd.read_parquet(
                file
            )
            for file in files
        ],
        ignore_index=True,
    )

    if len(df) != 1:

        raise RuntimeError(
            "Expected exactly one final TEST result. "
            f"Found {len(df)}."
        )

    return df.iloc[0]


def main():

    print()
    print("=" * 70)
    print("REGISTER FINAL RECOMMENDATION CHAMPION")
    print("=" * 70)

    row = download_metrics()

    model_name = str(
        row["model"]
    )

    personalized_weight = float(
        row[
            "personalized_weight"
        ]
    )

    popularity_weight = float(
        row[
            "popularity_weight"
        ]
    )

    config = {

        "model_name": (
            model_name
        ),

        "algorithm": (
            "item_item_cosine_plus_popularity"
        ),

        "model_version": "v1",

        "top_k": int(
            row["top_k"]
        ),

        "personalized_weight": (
            personalized_weight
        ),

        "popularity_weight": (
            popularity_weight
        ),

        "purchase_weight": 5.0,

        "product_view_weight": 1.0,

        "similarity": (
            "cosine"
        ),

        "selection": {
            "dataset": (
                "VALIDATION"
            ),

            "selection_metric": (
                "hit_rate_at_10"
            ),
        },

        "final_evaluation": {
            "dataset": "TEST",

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

            "map_at_10": float(
                row[
                    "map_at_10"
                ]
            ),

            "ndcg_at_10": float(
                row[
                    "ndcg_at_10"
                ]
            ),

            "catalog_coverage": float(
                row[
                    "catalog_coverage"
                ]
            ),
        },

        "created_at": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }

    ARTIFACT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    config_file = (
        ARTIFACT_DIR
        /
        "recommendation_champion.json"
    )

    with open(
        config_file,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            config,
            file,
            indent=2,
        )

    mlflow.set_tracking_uri(
        MLFLOW_TRACKING_URI
    )

    mlflow.set_experiment(
        EXPERIMENT_NAME
    )

    with mlflow.start_run(
        run_name=(
            f"final_{model_name}"
        )
    ) as run:

        mlflow.set_tags(
            {
                "project": (
                    "retailpulse-ai"
                ),

                "ml_problem": (
                    "product_recommendation"
                ),

                "run_role": (
                    "final_selected_recommender"
                ),

                "selected_using": (
                    "validation"
                ),

                "test_usage": (
                    "final_evaluation_only"
                ),

                "deployment_status": (
                    "champion"
                ),
            }
        )

        mlflow.log_params(
            {
                "model_name": (
                    model_name
                ),

                "algorithm": (
                    "item_item_cosine_plus_popularity"
                ),

                "personalized_weight": (
                    personalized_weight
                ),

                "popularity_weight": (
                    popularity_weight
                ),

                "top_k": int(
                    row["top_k"]
                ),

                "purchase_weight": 5.0,

                "product_view_weight": 1.0,

                "similarity": "cosine",
            }
        )

        mlflow.log_metrics(
            {
                "test_precision_at_10": float(
                    row[
                        "precision_at_10"
                    ]
                ),

                "test_recall_at_10": float(
                    row[
                        "recall_at_10"
                    ]
                ),

                "test_hit_rate_at_10": float(
                    row[
                        "hit_rate_at_10"
                    ]
                ),

                "test_map_at_10": float(
                    row[
                        "map_at_10"
                    ]
                ),

                "test_ndcg_at_10": float(
                    row[
                        "ndcg_at_10"
                    ]
                ),

                "test_catalog_coverage": float(
                    row[
                        "catalog_coverage"
                    ]
                ),

                "test_evaluated_customers": float(
                    row[
                        "evaluated_customers"
                    ]
                ),
            }
        )

        mlflow.log_artifact(
            str(
                config_file
            ),
            artifact_path=(
                "champion"
            ),
        )

        run_id = (
            run.info.run_id
        )

    print()
    print(
        "Champion configuration:",
        model_name,
    )

    print(
        "Personalized:",
        personalized_weight,
    )

    print(
        "Popularity:",
        popularity_weight,
    )

    print(
        "MLflow run:",
        run_id,
    )

    print(
        "Artifact:",
        config_file,
    )

    print()
    print("=" * 70)
    print("ML-2G COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()