from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import pandas as pd
from minio import Minio


# ============================================================
# CONFIGURATION
# ============================================================

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow:5000",
)

MLFLOW_EXPERIMENT_NAME = os.getenv(
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
    "ml/exports/recommendation_metrics/"
)

LOCAL_DIR = Path(
    "/tmp/recommendation_metrics"
)

ARTIFACT_DIR = Path(
    "/opt/retailpulse/ml/artifacts/recommendation"
)


# ============================================================
# DOWNLOAD METRICS
# ============================================================

def download_metrics() -> pd.DataFrame:

    print()
    print("Downloading recommendation metrics...")

    if LOCAL_DIR.exists():

        for file in LOCAL_DIR.glob("*"):
            if file.is_file():
                file.unlink()

    LOCAL_DIR.mkdir(
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
            "No recommendation metric Parquet "
            "files found in MinIO."
        )

    files = []

    for index, obj in enumerate(
        parquet_objects
    ):

        local_file = (
            LOCAL_DIR
            /
            f"part-{index:05d}.parquet"
        )

        client.fget_object(
            MINIO_BUCKET,
            obj.object_name,
            str(local_file),
        )

        files.append(
            local_file
        )

    frames = [
        pd.read_parquet(file)
        for file in files
    ]

    metrics = pd.concat(
        frames,
        ignore_index=True,
    )

    print(
        "Metric rows:",
        len(metrics),
    )

    return metrics


# ============================================================
# MODEL CONFIG
# ============================================================

def get_model_config(
    model_name: str,
) -> dict:

    configs = {

        "popularity": {
            "algorithm": (
                "global_popularity"
            ),
            "top_k": 10,
            "personalized_weight": 0.0,
            "popularity_weight": 1.0,
            "similarity": "none",
        },

        "item_item": {
            "algorithm": (
                "item_item_collaborative_filtering"
            ),
            "top_k": 10,
            "personalized_weight": 1.0,
            "popularity_weight": 0.0,
            "similarity": "cosine",
        },

        "hybrid_80_20": {
            "algorithm": (
                "hybrid_item_item_popularity"
            ),
            "top_k": 10,
            "personalized_weight": 0.80,
            "popularity_weight": 0.20,
            "similarity": "cosine",
        },
    }

    if model_name not in configs:

        raise RuntimeError(
            f"Unknown recommendation model: "
            f"{model_name}"
        )

    return configs[
        model_name
    ]


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 70)
    print("ML-2F RECOMMENDATION MLFLOW TRACKING")
    print("=" * 70)

    mlflow.set_tracking_uri(
        MLFLOW_TRACKING_URI
    )

    mlflow.set_experiment(
        MLFLOW_EXPERIMENT_NAME
    )

    metrics_df = download_metrics()

    required_columns = {
        "model",
        "evaluated_customers",
        "precision_at_10",
        "recall_at_10",
        "hit_rate_at_10",
        "map_at_10",
        "ndcg_at_10",
        "recommended_products",
        "catalog_coverage",
    }

    missing = (
        required_columns
        -
        set(
            metrics_df.columns
        )
    )

    if missing:

        raise RuntimeError(
            "Missing metric columns: "
            + ", ".join(
                sorted(missing)
            )
        )

    ARTIFACT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    experiment_summary = []

    # ========================================================
    # ONE MLFLOW RUN PER RECOMMENDER
    # ========================================================

    for _, row in metrics_df.iterrows():

        model_name = str(
            row["model"]
        )

        config = get_model_config(
            model_name
        )

        run_name = (
            f"{model_name}_baseline"
        )

        print()
        print("-" * 70)
        print(
            "Logging:",
            model_name,
        )
        print("-" * 70)

        with mlflow.start_run(
            run_name=run_name
        ) as run:

            # ================================================
            # TAGS
            # ================================================

            mlflow.set_tags(
                {
                    "project": "retailpulse-ai",
                    "ml_problem": (
                        "product_recommendation"
                    ),
                    "run_role": (
                        "baseline_evaluation"
                    ),
                    "evaluation_type": (
                        "time_based_holdout"
                    ),
                    "model_family": (
                        model_name
                    ),
                }
            )

            # ================================================
            # PARAMETERS
            # ================================================

            mlflow.log_params(
                config
            )

            # ================================================
            # METRICS
            # ================================================

            mlflow.log_metrics(
                {
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

                    "evaluated_customers": float(
                        row[
                            "evaluated_customers"
                        ]
                    ),

                    "recommended_products": float(
                        row[
                            "recommended_products"
                        ]
                    ),
                }
            )

            # ================================================
            # RUN METADATA
            # ================================================

            metadata = {

                "project": (
                    "retailpulse-ai"
                ),

                "model": (
                    model_name
                ),

                "configuration": (
                    config
                ),

                "evaluation": {
                    "type": (
                        "time_based_holdout"
                    ),
                    "top_k": 10,
                },

                "metrics": {
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

            metadata_file = (
                ARTIFACT_DIR
                /
                f"{model_name}_metadata.json"
            )

            with open(
                metadata_file,
                "w",
                encoding="utf-8",
            ) as f:

                json.dump(
                    metadata,
                    f,
                    indent=2,
                )

            mlflow.log_artifact(
                str(
                    metadata_file
                ),
                artifact_path=(
                    "metadata"
                ),
            )

            experiment_summary.append(
                {
                    "model": model_name,
                    "run_id": (
                        run.info.run_id
                    ),
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
                }
            )

            print(
                "Run ID:",
                run.info.run_id,
            )

    # ========================================================
    # SAVE EXPERIMENT SUMMARY
    # ========================================================

    summary_df = pd.DataFrame(
        experiment_summary
    )

    summary_file = (
        ARTIFACT_DIR
        /
        "recommendation_experiment_summary.csv"
    )

    summary_df.to_csv(
        summary_file,
        index=False,
    )

    print()
    print("=" * 70)
    print("EXPERIMENT SUMMARY")
    print("=" * 70)

    print(
        summary_df.to_string(
            index=False
        )
    )

    print()
    print(
        "IMPORTANT: These are baseline/test "
        "evaluation runs. They are not being "
        "used here to select a champion."
    )

    print()
    print("MLflow experiment:")
    print(
        MLFLOW_EXPERIMENT_NAME
    )


if __name__ == "__main__":
    main()