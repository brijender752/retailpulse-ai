from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from minio import Minio

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    OneHotEncoder,
    StandardScaler,
)


BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retailpulse",
)

PREFIX = (
    "ml/exports/churn_training/"
)

MODEL_DIR = Path(
    "/opt/retailpulse/ml/models"
)

ARTIFACT_DIR = Path(
    "/opt/retailpulse/ml/artifacts"
)


MODEL_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

ARTIFACT_DIR.mkdir(
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
        .replace("http://", "")
        .replace("https://", "")
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


def download_training_files():

    client = get_minio_client()

    download_dir = Path(
        "/tmp/churn_training"
    )

    download_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    files = []

    objects = client.list_objects(
        BUCKET,
        prefix=PREFIX,
        recursive=True,
    )

    for obj in objects:

        if not obj.object_name.endswith(
            ".parquet"
        ):
            continue

        local_path = (
            download_dir
            /
            Path(
                obj.object_name
            ).name
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
            "No churn training parquet "
            "files found in MinIO."
        )

    print(
        f"Downloaded {len(files)} "
        "training parquet files."
    )

    return files


def load_training_data():

    files = download_training_files()

    frames = [
        pd.read_parquet(file)
        for file in files
    ]

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    df["observation_date"] = (
        pd.to_datetime(
            df["observation_date"]
        )
    )

    df = df.sort_values(
        [
            "observation_date",
            "customer_id",
        ]
    ).reset_index(
        drop=True
    )

    return df


def time_split(df):

    dates = sorted(
        df[
            "observation_date"
        ].dropna().unique()
    )

    if len(dates) < 3:

        raise RuntimeError(
            "Need at least 3 distinct "
            "observation dates."
        )

    # Last 20% of observation dates
    # become test data.

    split_index = int(
        len(dates) * 0.8
    )

    split_index = min(
        max(split_index, 1),
        len(dates) - 1,
    )

    test_start_date = dates[
        split_index
    ]

    train_df = df[
        df["observation_date"]
        <
        test_start_date
    ].copy()

    test_df = df[
        df["observation_date"]
        >=
        test_start_date
    ].copy()

    print()
    print(
        "Train:",
        train_df[
            "observation_date"
        ].min(),
        "→",
        train_df[
            "observation_date"
        ].max(),
    )

    print(
        "Test:",
        test_df[
            "observation_date"
        ].min(),
        "→",
        test_df[
            "observation_date"
        ].max(),
    )

    return train_df, test_df


def build_preprocessor(
    numeric_features,
    categorical_features,
):

    numeric_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="median"
                ),
            ),
            (
                "scaler",
                StandardScaler(),
            ),
        ]
    )

    categorical_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="most_frequent"
                ),
            ),
            (
                "encoder",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=False,
                ),
            ),
        ]
    )

    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                numeric_pipeline,
                numeric_features,
            ),
            (
                "categorical",
                categorical_pipeline,
                categorical_features,
            ),
        ]
    )


def calculate_metrics(
    name,
    pipeline,
    X_test,
    y_test,
):

    prediction = pipeline.predict(
        X_test
    )

    probability = (
        pipeline.predict_proba(
            X_test
        )[:, 1]
    )

    metrics = {
        "model": name,

        "accuracy": float(
            accuracy_score(
                y_test,
                prediction,
            )
        ),

        "precision": float(
            precision_score(
                y_test,
                prediction,
                zero_division=0,
            )
        ),

        "recall": float(
            recall_score(
                y_test,
                prediction,
                zero_division=0,
            )
        ),

        "f1": float(
            f1_score(
                y_test,
                prediction,
                zero_division=0,
            )
        ),

        "roc_auc": float(
            roc_auc_score(
                y_test,
                probability,
            )
        ),

        "pr_auc": float(
            average_precision_score(
                y_test,
                probability,
            )
        ),
    }

    print()
    print("=" * 70)
    print(name)
    print("=" * 70)

    for key, value in metrics.items():

        if key != "model":
            print(
                f"{key:12}: "
                f"{value:.4f}"
            )

    print()
    print(
        classification_report(
            y_test,
            prediction,
            digits=4,
            zero_division=0,
        )
    )

    print(
        "Confusion matrix:"
    )

    print(
        confusion_matrix(
            y_test,
            prediction,
        )
    )

    return (
        metrics,
        prediction,
        probability,
    )


def main():

    df = load_training_data()

    print()
    print(
        "Training rows:",
        len(df),
    )

    print(
        "Observation dates:",
        df[
            "observation_date"
        ].nunique(),
    )

    print()
    print(
        "Target distribution:"
    )

    print(
        df["churned"]
        .value_counts(
            normalize=True
        )
        .sort_index()
    )

    # -------------------------------------------------
    # NEVER use these as features
    # -------------------------------------------------

    excluded_columns = {
        "snapshot_id",
        "customer_id",
        "observation_date",

        # leakage
        "future_orders_60d",

        # target
        "churned",

        # raw date
        "signup_date",
        "last_order_date",
    }

    categorical_features = [
        column
        for column in [
            "customer_segment",
            "country",
            "state",
        ]
        if column in df.columns
    ]

    numeric_features = [
        column
        for column in df.columns
        if (
            column
            not in excluded_columns
            and
            column
            not in categorical_features
            and
            pd.api.types.is_numeric_dtype(
                df[column]
            )
        )
    ]

    feature_columns = (
        numeric_features
        +
        categorical_features
    )

    print()
    print(
        "Numeric features:"
    )

    for feature in numeric_features:
        print(" -", feature)

    print()
    print(
        "Categorical features:"
    )

    for feature in categorical_features:
        print(" -", feature)

    train_df, test_df = (
        time_split(df)
    )

    X_train = train_df[
        feature_columns
    ]

    y_train = (
        train_df["churned"]
        .astype(int)
    )

    X_test = test_df[
        feature_columns
    ]

    y_test = (
        test_df["churned"]
        .astype(int)
    )

    if y_train.nunique() < 2:

        raise RuntimeError(
            "Training data contains "
            "only one churn class."
        )

    if y_test.nunique() < 2:

        raise RuntimeError(
            "Test data contains "
            "only one churn class."
        )

    print()
    print(
        "Train rows:",
        len(train_df)
    )

    print(
        "Test rows:",
        len(test_df)
    )

    # -------------------------------------------------
    # Models
    # -------------------------------------------------

    models = {

        "logistic_regression":
            LogisticRegression(
                max_iter=2000,
                class_weight="balanced",
                random_state=42,
            ),

        "random_forest":
            RandomForestClassifier(
                n_estimators=300,
                max_depth=12,
                min_samples_leaf=5,
                class_weight="balanced",
                n_jobs=-1,
                random_state=42,
            ),

        "hist_gradient_boosting":
            HistGradientBoostingClassifier(
                learning_rate=0.08,
                max_iter=250,
                max_leaf_nodes=31,
                random_state=42,
            ),
    }

    results = []

    trained_models = {}

    predictions = {}

    for name, model in models.items():

        preprocessor = (
            build_preprocessor(
                numeric_features,
                categorical_features,
            )
        )

        pipeline = Pipeline(
            steps=[
                (
                    "preprocessor",
                    preprocessor,
                ),
                (
                    "classifier",
                    model,
                ),
            ]
        )

        print()
        print(
            "Training:",
            name
        )

        pipeline.fit(
            X_train,
            y_train,
        )

        (
            metrics,
            prediction,
            probability,
        ) = calculate_metrics(
            name,
            pipeline,
            X_test,
            y_test,
        )

        results.append(
            metrics
        )

        trained_models[
            name
        ] = pipeline

        predictions[
            name
        ] = (
            prediction,
            probability,
        )

    # -------------------------------------------------
    # Compare
    # -------------------------------------------------

    results_df = pd.DataFrame(
        results
    )

    results_df = (
        results_df
        .sort_values(
            "pr_auc",
            ascending=False,
        )
        .reset_index(
            drop=True
        )
    )

    print()
    print("=" * 70)
    print("MODEL COMPARISON")
    print("=" * 70)

    print(
        results_df.to_string(
            index=False
        )
    )

    # -------------------------------------------------
    # Select best by PR-AUC
    # -------------------------------------------------

    best_model_name = (
        results_df.iloc[0][
            "model"
        ]
    )

    best_model = (
        trained_models[
            best_model_name
        ]
    )

    print()
    print(
        "Selected model:",
        best_model_name,
    )

    # -------------------------------------------------
    # Save model
    # -------------------------------------------------

    model_path = (
        MODEL_DIR
        /
        "churn_model.joblib"
    )

    joblib.dump(
        best_model,
        model_path,
    )

    # -------------------------------------------------
    # Save metadata
    # -------------------------------------------------

    metadata = {
        "model_name":
            best_model_name,

        "feature_columns":
            feature_columns,

        "numeric_features":
            numeric_features,

        "categorical_features":
            categorical_features,

        "target":
            "churned",

        "selection_metric":
            "pr_auc",

        "train_rows":
            len(train_df),

        "test_rows":
            len(test_df),

        "test_start_date":
            str(
                test_df[
                    "observation_date"
                ].min()
            ),
    }

    metadata_path = (
        MODEL_DIR
        /
        "churn_model_metadata.json"
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

    # -------------------------------------------------
    # Save metrics
    # -------------------------------------------------

    results_df.to_csv(
        ARTIFACT_DIR
        /
        "churn_model_comparison.csv",
        index=False,
    )

    # -------------------------------------------------
    # Save test predictions
    # -------------------------------------------------

    best_prediction, best_probability = (
        predictions[
            best_model_name
        ]
    )

    prediction_output = test_df[
        [
            "snapshot_id",
            "customer_id",
            "observation_date",
            "churned",
        ]
    ].copy()

    prediction_output[
        "churn_probability"
    ] = best_probability

    prediction_output[
        "prediction"
    ] = best_prediction

    prediction_output.to_csv(
        ARTIFACT_DIR
        /
        "churn_test_predictions.csv",
        index=False,
    )

    print()
    print("=" * 70)
    print("SUCCESS")
    print("=" * 70)

    print(
        "Model:",
        model_path,
    )

    print(
        "Metadata:",
        metadata_path,
    )


if __name__ == "__main__":
    main()