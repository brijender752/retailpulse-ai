from __future__ import annotations

import json
import os
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd

from minio import Minio
from mlflow import MlflowClient


BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retailpulse",
)

INPUT_PREFIX = (
    "ml/exports/churn_scoring_features/"
)

OUTPUT_PREFIX = (
    "ml/predictions/churn/"
)

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow:5000",
)

REGISTERED_MODEL_NAME = os.getenv(
    "MLFLOW_REGISTERED_MODEL_NAME",
    "RetailPulseChurnModel",
)

MODEL_ALIAS = os.getenv(
    "MLFLOW_MODEL_ALIAS",
    "champion",
)


DOWNLOAD_DIR = Path(
    "/tmp/churn_scoring"
)

OUTPUT_DIR = Path(
    "/tmp/churn_predictions"
)

DOWNLOAD_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


def get_minio_client():

    endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "minio:9000",
    )

    endpoint = (
        endpoint
        .replace(
            "http://",
            "",
        )
        .replace(
            "https://",
            "",
        )
    )

    return Minio(
        endpoint,
        access_key=os.getenv(
            "MINIO_ACCESS_KEY",
            "minioadmin",
        ),
        secret_key=os.getenv(
            "MINIO_SECRET_KEY",
            "minioadmin",
        ),
        secure=False,
    )


def download_features():

    client = get_minio_client()

    files = []

    for obj in client.list_objects(
        BUCKET,
        prefix=INPUT_PREFIX,
        recursive=True,
    ):

        if not obj.object_name.endswith(
            ".parquet"
        ):
            continue

        local_path = (
            DOWNLOAD_DIR
            /
            obj.object_name.replace(
                "/",
                "__",
            )
        )

        client.fget_object(
            BUCKET,
            obj.object_name,
            str(local_path),
        )

        files.append(
            local_path
        )

    if not files:

        raise RuntimeError(
            "No churn scoring "
            "Parquet files found."
        )

    return files


def load_features():

    files = download_features()

    frames = [
        pd.read_parquet(
            file
        )
        for file in files
    ]

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    if df.empty:

        raise RuntimeError(
            "Scoring dataset is empty."
        )

    return df


def get_model_information():

    client = MlflowClient()

    version = (
        client
        .get_model_version_by_alias(
            REGISTERED_MODEL_NAME,
            MODEL_ALIAS,
        )
    )

    return version


def load_model():

    mlflow.set_tracking_uri(
        MLFLOW_TRACKING_URI
    )

    model_uri = (
        f"models:/"
        f"{REGISTERED_MODEL_NAME}"
        f"@{MODEL_ALIAS}"
    )

    print(
        "Loading model:",
        model_uri,
    )

    model = (
        mlflow.sklearn.load_model(
            model_uri
        )
    )

    return (
        model,
        model_uri,
    )


def risk_band(
    probability: float,
) -> str:

    if probability >= 0.80:
        return "CRITICAL"

    if probability >= 0.60:
        return "HIGH"

    if probability >= 0.30:
        return "MEDIUM"

    return "LOW"


def upload_predictions(
    output_path: Path,
):

    client = get_minio_client()

    object_name = (
        OUTPUT_PREFIX
        +
        "churn_predictions.parquet"
    )

    client.fput_object(
        BUCKET,
        object_name,
        str(output_path),
    )

    print(
        "Uploaded:",
        f"s3://{BUCKET}/{object_name}",
    )


def main():

    mlflow.set_tracking_uri(
        MLFLOW_TRACKING_URI
    )

    print()
    print("=" * 70)
    print("RETAILPULSE CHURN INFERENCE")
    print("=" * 70)

    # ========================================================
    # LOAD FEATURES
    # ========================================================

    df = load_features()

    print(
        "Customers:",
        len(df),
    )

    # ========================================================
    # LOAD MODEL
    # ========================================================

    (
        model,
        model_uri,
    ) = load_model()

    model_info = (
        get_model_information()
    )

    model_version = str(
        model_info.version
    )

    print(
        "Model version:",
        model_version,
    )

    # ========================================================
    # GET MODEL FEATURE LIST
    # ========================================================
    #
    # sklearn ColumnTransformer stores the columns that were
    # configured during training.
    # ========================================================

    preprocessor = (
        model.named_steps[
            "preprocessor"
        ]
    )

    feature_columns = []

    for (
        name,
        transformer,
        columns,
    ) in preprocessor.transformers_:

        if name == "remainder":
            continue

        feature_columns.extend(
            list(columns)
        )

    print()
    print(
        "Expected features:",
        len(feature_columns),
    )

    missing = [
        column
        for column in feature_columns
        if column not in df.columns
    ]

    if missing:

        raise RuntimeError(
            "Scoring data is missing "
            "model features: "
            +
            ", ".join(
                missing
            )
        )

    X = df[
        feature_columns
    ].copy()

    # ========================================================
    # INFERENCE
    # ========================================================

    probability = (
        model.predict_proba(
            X
        )[:, 1]
    )

    prediction = (
        model.predict(
            X
        )
    )

    # ========================================================
    # OUTPUT
    # ========================================================

    result = pd.DataFrame(
        {
            "customer_id":
                df[
                    "customer_id"
                ],

            "prediction_date":
                df[
                    "prediction_date"
                ],

            "churn_probability":
                probability,

            "churn_prediction":
                prediction,
        }
    )

    result[
        "risk_band"
    ] = (
        result[
            "churn_probability"
        ]
        .apply(
            risk_band
        )
    )

    # ========================================================
    # REVENUE AT RISK
    # ========================================================
    #
    # We use recent 180-day revenue as the current customer
    # value proxy.
    #
    # Expected revenue at risk:
    #
    # revenue_180d × churn_probability
    # ========================================================

    result[
        "revenue_180d"
    ] = (
        df[
            "revenue_180d"
        ]
        .fillna(0)
        .astype(float)
    )

    result[
        "revenue_at_risk"
    ] = (
        result[
            "revenue_180d"
        ]
        *
        result[
            "churn_probability"
        ]
    )

    result[
        "model_name"
    ] = (
        REGISTERED_MODEL_NAME
    )

    result[
        "model_version"
    ] = (
        model_version
    )

    result[
        "model_alias"
    ] = (
        MODEL_ALIAS
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    if (
        (
            result[
                "churn_probability"
            ] < 0
        )
        |
        (
            result[
                "churn_probability"
            ] > 1
        )
    ).any():

        raise RuntimeError(
            "Invalid churn probability."
        )

    print()
    print("=" * 70)
    print("PREDICTION DISTRIBUTION")
    print("=" * 70)

    print(
        result[
            "risk_band"
        ]
        .value_counts()
    )

    print()

    print(
        "Average churn probability:",
        round(
            result[
                "churn_probability"
            ].mean(),
            4,
        ),
    )

    print(
        "Predicted churn rate:",
        round(
            result[
                "churn_prediction"
            ].mean(),
            4,
        ),
    )

    print(
        "Revenue at risk:",
        round(
            result[
                "revenue_at_risk"
            ].sum(),
            2,
        ),
    )

    # ========================================================
    # SAVE
    # ========================================================

    output_path = (
        OUTPUT_DIR
        /
        "churn_predictions.parquet"
    )

    result.to_parquet(
        output_path,
        index=False,
    )

    upload_predictions(
        output_path
    )

    metadata = {
        "registered_model":
            REGISTERED_MODEL_NAME,

        "model_version":
            model_version,

        "model_alias":
            MODEL_ALIAS,

        "model_uri":
            model_uri,

        "customers_scored":
            len(result),

        "average_churn_probability":
            float(
                result[
                    "churn_probability"
                ].mean()
            ),

        "revenue_at_risk":
            float(
                result[
                    "revenue_at_risk"
                ].sum()
            ),
    }

    metadata_path = (
        OUTPUT_DIR
        /
        "prediction_metadata.json"
    )

    with open(
        metadata_path,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            metadata,
            file,
            indent=2,
        )

    print()
    print("=" * 70)
    print("SUCCESS")
    print("=" * 70)

    print(
        "Customers scored:",
        len(result),
    )

    print(
        "Model:",
        model_uri,
    )

    print(
        "Version:",
        model_version,
    )


if __name__ == "__main__":
    main()