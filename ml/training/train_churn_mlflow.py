from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd

from minio import Minio

from mlflow import MlflowClient

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


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retailpulse",
)

TRAINING_PREFIX = os.getenv(
    "CHURN_TRAINING_PREFIX",
    "ml/exports/churn_training/",
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin",
)


MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow:5000",
)

MLFLOW_EXPERIMENT_NAME = os.getenv(
    "MLFLOW_EXPERIMENT_NAME",
    "retailpulse-churn",
)

REGISTERED_MODEL_NAME = os.getenv(
    "MLFLOW_REGISTERED_MODEL_NAME",
    "RetailPulseChurnModel",
)


MODEL_DIR = Path(
    "/opt/retailpulse/ml/models"
)

ARTIFACT_DIR = Path(
    "/opt/retailpulse/ml/artifacts"
)

DOWNLOAD_DIR = Path(
    "/tmp/churn_training"
)


MODEL_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

ARTIFACT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

DOWNLOAD_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# ============================================================
# MINIO
# ============================================================

def get_minio_client() -> Minio:
    """
    Create MinIO client.
    """

    endpoint = (
        MINIO_ENDPOINT
        .replace("http://", "")
        .replace("https://", "")
    )

    secure = MINIO_ENDPOINT.startswith(
        "https://"
    )

    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )


# ============================================================
# DOWNLOAD TRAINING DATA
# ============================================================

def download_training_files() -> list[Path]:
    """
    Download churn training parquet files from MinIO.
    """

    client = get_minio_client()

    files: list[Path] = []

    print()
    print("=" * 70)
    print("DOWNLOADING TRAINING DATA")
    print("=" * 70)

    print("Bucket:", BUCKET)
    print("Prefix:", TRAINING_PREFIX)

    objects = client.list_objects(
        BUCKET,
        prefix=TRAINING_PREFIX,
        recursive=True,
    )

    for obj in objects:

        if not obj.object_name.endswith(
            ".parquet"
        ):
            continue

        # Avoid collisions if Spark somehow produces
        # files with identical base names.
        safe_name = (
            obj.object_name
            .replace("/", "__")
        )

        local_path = (
            DOWNLOAD_DIR
            /
            safe_name
        )

        print(
            "Downloading:",
            obj.object_name,
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
            "No Parquet files were found in "
            f"s3://{BUCKET}/{TRAINING_PREFIX}"
        )

    print()
    print(
        f"Downloaded {len(files)} "
        "Parquet file(s)."
    )

    return files


# ============================================================
# LOAD TRAINING DATA
# ============================================================

def load_training_data() -> pd.DataFrame:
    """
    Load all downloaded parquet files into Pandas.
    """

    files = download_training_files()

    frames = []

    for file in files:

        print(
            "Reading:",
            file
        )

        frame = pd.read_parquet(
            file
        )

        frames.append(
            frame
        )

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    if df.empty:
        raise RuntimeError(
            "Churn training dataset is empty."
        )

    required_columns = {
        "customer_id",
        "observation_date",
        "churned",
    }

    missing_columns = (
        required_columns
        -
        set(df.columns)
    )

    if missing_columns:

        raise RuntimeError(
            "Missing required columns: "
            + ", ".join(
                sorted(missing_columns)
            )
        )

    df[
        "observation_date"
    ] = pd.to_datetime(
        df["observation_date"],
        errors="coerce",
    )

    if (
        df["observation_date"]
        .isna()
        .any()
    ):
        raise RuntimeError(
            "Some observation_date values "
            "could not be converted to dates."
        )

    df["churned"] = (
        pd.to_numeric(
            df["churned"],
            errors="raise",
        )
        .astype(int)
    )

    df = (
        df
        .sort_values(
            [
                "observation_date",
                "customer_id",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    print()
    print("=" * 70)
    print("TRAINING DATA")
    print("=" * 70)

    print(
        "Rows:",
        f"{len(df):,}",
    )

    print(
        "Columns:",
        len(df.columns),
    )

    print(
        "Observation dates:",
        df[
            "observation_date"
        ].nunique(),
    )

    print(
        "First observation:",
        df[
            "observation_date"
        ].min(),
    )

    print(
        "Last observation:",
        df[
            "observation_date"
        ].max(),
    )

    print()
    print(
        "Target distribution:"
    )

    print(
        df[
            "churned"
        ]
        .value_counts(
            normalize=False
        )
        .sort_index()
    )

    print()
    print(
        "Target percentage:"
    )

    print(
        (
            df[
                "churned"
            ]
            .value_counts(
                normalize=True
            )
            .sort_index()
            * 100
        ).round(2)
    )

    return df


# ============================================================
# TIME-BASED SPLIT
# ============================================================

def time_split(
    df: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    """
    Split using observation dates.

    Oldest ~80% observation dates -> training
    Newest ~20% observation dates -> testing

    We intentionally do NOT use train_test_split().
    """

    dates = sorted(
        df[
            "observation_date"
        ]
        .dropna()
        .unique()
    )

    if len(dates) < 3:
        raise RuntimeError(
            "At least 3 distinct observation dates "
            "are required for a time-based split."
        )

    split_index = int(
        len(dates) * 0.80
    )

    split_index = min(
        max(
            split_index,
            1,
        ),
        len(dates) - 1,
    )

    test_start_date = dates[
        split_index
    ]

    train_df = (
        df[
            df[
                "observation_date"
            ]
            <
            test_start_date
        ]
        .copy()
    )

    test_df = (
        df[
            df[
                "observation_date"
            ]
            >=
            test_start_date
        ]
        .copy()
    )

    if train_df.empty:
        raise RuntimeError(
            "Training split is empty."
        )

    if test_df.empty:
        raise RuntimeError(
            "Test split is empty."
        )

    if (
        train_df["churned"]
        .nunique()
        <
        2
    ):
        raise RuntimeError(
            "Training split contains only "
            "one churn class."
        )

    if (
        test_df["churned"]
        .nunique()
        <
        2
    ):
        raise RuntimeError(
            "Test split contains only "
            "one churn class."
        )

    print()
    print("=" * 70)
    print("TIME-BASED SPLIT")
    print("=" * 70)

    print(
        "Training period:",
        train_df[
            "observation_date"
        ].min(),
        "→",
        train_df[
            "observation_date"
        ].max(),
    )

    print(
        "Testing period:",
        test_df[
            "observation_date"
        ].min(),
        "→",
        test_df[
            "observation_date"
        ].max(),
    )

    print()

    print(
        "Training rows:",
        f"{len(train_df):,}",
    )

    print(
        "Test rows:",
        f"{len(test_df):,}",
    )

    print()

    print(
        "Training churn rate:",
        round(
            train_df[
                "churned"
            ].mean(),
            4,
        ),
    )

    print(
        "Test churn rate:",
        round(
            test_df[
                "churned"
            ].mean(),
            4,
        ),
    )

    return (
        train_df,
        test_df,
    )


# ============================================================
# FEATURE DISCOVERY
# ============================================================

def get_features(
    df: pd.DataFrame,
) -> tuple[
    list[str],
    list[str],
    list[str],
]:
    """
    Determine numeric/categorical model features.

    IMPORTANT:
    future_orders_60d and churned must NEVER
    become model input features.
    """

    excluded_columns = {
        "snapshot_id",
        "customer_id",
        "observation_date",

        # Leakage
        "future_orders_60d",

        # Target
        "churned",

        # Raw dates, if present
        "signup_date",
        "last_order_date",
        "last_payment_date",
    }

    categorical_candidates = [
        "customer_segment",
        "country",
        "state",
    ]

    categorical_features = [
        column
        for column in categorical_candidates
        if column in df.columns
    ]

    numeric_features = []

    for column in df.columns:

        if column in excluded_columns:
            continue

        if column in categorical_features:
            continue

        if pd.api.types.is_numeric_dtype(
            df[column]
        ):
            numeric_features.append(
                column
            )

    feature_columns = (
        numeric_features
        +
        categorical_features
    )

    if not feature_columns:
        raise RuntimeError(
            "No model features were discovered."
        )

    # Explicit leakage safety check.
    forbidden = {
        "future_orders_60d",
        "churned",
    }

    leaked = (
        forbidden
        &
        set(feature_columns)
    )

    if leaked:
        raise RuntimeError(
            "LEAKAGE DETECTED. "
            "Forbidden model features: "
            + ", ".join(
                sorted(leaked)
            )
        )

    print()
    print("=" * 70)
    print("MODEL FEATURES")
    print("=" * 70)

    print()
    print("Numeric features:")

    for feature in numeric_features:
        print(
            " -",
            feature
        )

    print()
    print(
        "Categorical features:"
    )

    for feature in categorical_features:
        print(
            " -",
            feature
        )

    print()
    print(
        "Total features:",
        len(feature_columns),
    )

    return (
        feature_columns,
        numeric_features,
        categorical_features,
    )


# ============================================================
# PREPROCESSOR
# ============================================================

def build_preprocessor(
    numeric_features: list[str],
    categorical_features: list[str],
) -> ColumnTransformer:

    transformers = []

    if numeric_features:

        numeric_pipeline = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(
                        strategy="median",
                    ),
                ),
                (
                    "scaler",
                    StandardScaler(),
                ),
            ]
        )

        transformers.append(
            (
                "numeric",
                numeric_pipeline,
                numeric_features,
            )
        )

    if categorical_features:

        categorical_pipeline = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(
                        strategy="most_frequent",
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

        transformers.append(
            (
                "categorical",
                categorical_pipeline,
                categorical_features,
            )
        )

    return ColumnTransformer(
        transformers=transformers,
        remainder="drop",
    )


# ============================================================
# MODEL DEFINITIONS
# ============================================================

def get_models():
    """
    Baseline model candidates.
    """

    return {

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


# ============================================================
# EVALUATION
# ============================================================

def evaluate_model(
    model_name: str,
    pipeline: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
):
    """
    Evaluate a fitted sklearn pipeline.
    """

    predictions = pipeline.predict(
        X_test
    )

    probabilities = (
        pipeline.predict_proba(
            X_test
        )[:, 1]
    )

    metrics = {

        "model":
            model_name,

        "accuracy":
            float(
                accuracy_score(
                    y_test,
                    predictions,
                )
            ),

        "precision":
            float(
                precision_score(
                    y_test,
                    predictions,
                    zero_division=0,
                )
            ),

        "recall":
            float(
                recall_score(
                    y_test,
                    predictions,
                    zero_division=0,
                )
            ),

        "f1":
            float(
                f1_score(
                    y_test,
                    predictions,
                    zero_division=0,
                )
            ),

        "roc_auc":
            float(
                roc_auc_score(
                    y_test,
                    probabilities,
                )
            ),

        "pr_auc":
            float(
                average_precision_score(
                    y_test,
                    probabilities,
                )
            ),
    }

    report = classification_report(
        y_test,
        predictions,
        digits=4,
        zero_division=0,
    )

    matrix = confusion_matrix(
        y_test,
        predictions,
    )

    print()
    print("-" * 70)
    print(
        "EVALUATION:",
        model_name,
    )
    print("-" * 70)

    for key in [
        "accuracy",
        "precision",
        "recall",
        "f1",
        "roc_auc",
        "pr_auc",
    ]:

        print(
            f"{key:12}: "
            f"{metrics[key]:.4f}"
        )

    print()
    print(
        "Classification report:"
    )

    print(
        report
    )

    print(
        "Confusion matrix:"
    )

    print(
        matrix
    )

    return (
        metrics,
        predictions,
        probabilities,
        report,
        matrix,
    )


# ============================================================
# SAVE LOCAL EVALUATION ARTIFACTS
# ============================================================

def save_run_artifacts(
    model_name: str,
    metrics: dict,
    report: str,
    matrix,
):
    """
    Save evaluation artifacts locally so they can
    also be uploaded to MLflow.
    """

    run_dir = (
        ARTIFACT_DIR
        /
        model_name
    )

    run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metrics_path = (
        run_dir
        /
        "metrics.json"
    )

    report_path = (
        run_dir
        /
        "classification_report.txt"
    )

    matrix_path = (
        run_dir
        /
        "confusion_matrix.json"
    )

    with open(
        metrics_path,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            metrics,
            file,
            indent=2,
        )

    with open(
        report_path,
        "w",
        encoding="utf-8",
    ) as file:

        file.write(
            report
        )

    with open(
        matrix_path,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            matrix.tolist(),
            file,
            indent=2,
        )

    return {
        "metrics":
            metrics_path,

        "classification_report":
            report_path,

        "confusion_matrix":
            matrix_path,
    }


# ============================================================
# MLFLOW CONFIGURATION
# ============================================================

def configure_mlflow():
    """
    Configure MLflow tracking and experiment.
    """

    print()
    print("=" * 70)
    print("MLFLOW")
    print("=" * 70)

    print(
        "Tracking URI:",
        MLFLOW_TRACKING_URI,
    )

    print(
        "Experiment:",
        MLFLOW_EXPERIMENT_NAME,
    )

    print(
        "Registered model:",
        REGISTERED_MODEL_NAME,
    )

    mlflow.set_tracking_uri(
        MLFLOW_TRACKING_URI
    )

    mlflow.set_experiment(
        MLFLOW_EXPERIMENT_NAME
    )


# ============================================================
# MAIN TRAINING
# ============================================================

def main():

    # --------------------------------------------------------
    # MLflow
    # --------------------------------------------------------

    configure_mlflow()

    # --------------------------------------------------------
    # Data
    # --------------------------------------------------------

    df = load_training_data()

    (
        feature_columns,
        numeric_features,
        categorical_features,
    ) = get_features(
        df
    )

    (
        train_df,
        test_df,
    ) = time_split(
        df
    )

    X_train = train_df[
        feature_columns
    ].copy()

    y_train = (
        train_df[
            "churned"
        ]
        .astype(int)
    )

    X_test = test_df[
        feature_columns
    ].copy()

    y_test = (
        test_df[
            "churned"
        ]
        .astype(int)
    )

    # --------------------------------------------------------
    # Baseline information
    # --------------------------------------------------------

    train_churn_rate = float(
        y_train.mean()
    )

    test_churn_rate = float(
        y_test.mean()
    )

    print()
    print("=" * 70)
    print("BASELINE")
    print("=" * 70)

    print(
        "Training churn rate:",
        round(
            train_churn_rate,
            4,
        ),
    )

    print(
        "Test churn rate:",
        round(
            test_churn_rate,
            4,
        ),
    )

    # --------------------------------------------------------
    # Train models
    # --------------------------------------------------------

    models = get_models()

    results = []

    trained_models = {}

    run_ids = {}

    test_predictions = {}

    for (
        model_name,
        classifier,
    ) in models.items():

        print()
        print("=" * 70)
        print(
            "TRAINING:",
            model_name,
        )
        print("=" * 70)

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
                    classifier,
                ),
            ]
        )

        # ----------------------------------------------------
        # Start MLflow run
        # ----------------------------------------------------

        with mlflow.start_run(
            run_name=model_name
        ) as run:

            run_id = (
                run.info.run_id
            )

            print(
                "MLflow Run ID:",
                run_id,
            )

            # ------------------------------------------------
            # Tags
            # ------------------------------------------------

            mlflow.set_tags(
                {
                    "project":
                        "RetailPulse AI",

                    "problem":
                        "customer_churn",

                    "model_type":
                        model_name,

                    "split_strategy":
                        "time_based",

                    "target":
                        "churned",

                    "selection_metric":
                        "pr_auc",
                }
            )

            # ------------------------------------------------
            # General parameters
            # ------------------------------------------------

            mlflow.log_param(
                "model_name",
                model_name,
            )

            mlflow.log_param(
                "training_rows",
                len(train_df),
            )

            mlflow.log_param(
                "test_rows",
                len(test_df),
            )

            mlflow.log_param(
                "feature_count",
                len(feature_columns),
            )

            mlflow.log_param(
                "numeric_feature_count",
                len(
                    numeric_features
                ),
            )

            mlflow.log_param(
                "categorical_feature_count",
                len(
                    categorical_features
                ),
            )

            mlflow.log_param(
                "split_strategy",
                "time_based_80_20",
            )

            mlflow.log_param(
                "test_start_date",
                str(
                    test_df[
                        "observation_date"
                    ].min()
                ),
            )

            mlflow.log_param(
                "train_churn_rate",
                train_churn_rate,
            )

            mlflow.log_param(
                "test_churn_rate",
                test_churn_rate,
            )

            # ------------------------------------------------
            # Classifier parameters
            # ------------------------------------------------

            classifier_params = (
                classifier.get_params()
            )

            for (
                parameter,
                value,
            ) in classifier_params.items():

                # Keep parameter names clearly namespaced.
                try:
                    mlflow.log_param(
                        f"classifier__{parameter}",
                        value,
                    )
                except Exception:
                    # Some sklearn parameters may not serialize
                    # cleanly. They are not required to stop
                    # the entire training job.
                    pass

            # ------------------------------------------------
            # Train
            # ------------------------------------------------

            pipeline.fit(
                X_train,
                y_train,
            )

            # ------------------------------------------------
            # Evaluate
            # ------------------------------------------------

            (
                metrics,
                predictions,
                probabilities,
                report,
                matrix,
            ) = evaluate_model(
                model_name,
                pipeline,
                X_test,
                y_test,
            )

            # ------------------------------------------------
            # Log metrics
            # ------------------------------------------------

            for metric_name in [
                "accuracy",
                "precision",
                "recall",
                "f1",
                "roc_auc",
                "pr_auc",
            ]:

                mlflow.log_metric(
                    metric_name,
                    metrics[
                        metric_name
                    ],
                )

            # ------------------------------------------------
            # Feature metadata
            # ------------------------------------------------

            mlflow.log_dict(
                {
                    "feature_columns":
                        feature_columns,

                    "numeric_features":
                        numeric_features,

                    "categorical_features":
                        categorical_features,

                    "excluded_columns": [
                        "snapshot_id",
                        "customer_id",
                        "observation_date",
                        "future_orders_60d",
                        "churned",
                        "signup_date",
                        "last_order_date",
                        "last_payment_date",
                    ],

                    "target":
                        "churned",

                    "leakage_column":
                        "future_orders_60d",
                },
                "metadata/feature_metadata.json",
            )

            # ------------------------------------------------
            # Save evaluation files
            # ------------------------------------------------

            local_artifacts = (
                save_run_artifacts(
                    model_name,
                    metrics,
                    report,
                    matrix,
                )
            )

            mlflow.log_artifact(
                str(
                    local_artifacts[
                        "classification_report"
                    ]
                ),
                artifact_path="evaluation",
            )

            mlflow.log_artifact(
                str(
                    local_artifacts[
                        "confusion_matrix"
                    ]
                ),
                artifact_path="evaluation",
            )

            mlflow.log_artifact(
                str(
                    local_artifacts[
                        "metrics"
                    ]
                ),
                artifact_path="evaluation",
            )

            # ------------------------------------------------
            # Save model to MLflow
            # ------------------------------------------------

            skops_trusted_types = ["numpy.dtype"]
            if model_name == "random_forest":
                # This tree is created by the local RandomForest fit above.
                skops_trusted_types.append("sklearn.tree._tree.Tree")
            elif model_name == "hist_gradient_boosting":
                # This predictor is created by the local HistGradientBoosting fit.
                skops_trusted_types.append(
                    "sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor"
                )

            mlflow.sklearn.log_model(
                sk_model=pipeline,
                name="churn_model",
                skops_trusted_types=skops_trusted_types,
            )

            # ------------------------------------------------
            # Keep information for final comparison
            # ------------------------------------------------

            results.append(
                metrics
            )

            trained_models[
                model_name
            ] = pipeline

            run_ids[
                model_name
            ] = run_id

            test_predictions[
                model_name
            ] = {
                "prediction":
                    predictions,

                "probability":
                    probabilities,
            }

    # ========================================================
    # MODEL COMPARISON
    # ========================================================

    results_df = (
        pd.DataFrame(
            results
        )
        .sort_values(
            [
                "pr_auc",
                "roc_auc",
                "f1",
            ],
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

    comparison_path = (
        ARTIFACT_DIR
        /
        "churn_model_comparison.csv"
    )

    results_df.to_csv(
        comparison_path,
        index=False,
    )

    # ========================================================
    # SELECT BEST MODEL
    # ========================================================

    best_model_name = str(
        results_df.iloc[0][
            "model"
        ]
    )

    best_run_id = (
        run_ids[
            best_model_name
        ]
    )

    best_pipeline = (
        trained_models[
            best_model_name
        ]
    )

    best_metrics = (
        results_df.iloc[0]
        .to_dict()
    )

    print()
    print("=" * 70)
    print("SELECTED MODEL")
    print("=" * 70)

    print(
        "Model:",
        best_model_name,
    )

    print(
        "Run ID:",
        best_run_id,
    )

    print(
        "PR-AUC:",
        round(
            float(
                best_metrics[
                    "pr_auc"
                ]
            ),
            4,
        ),
    )

    print(
        "ROC-AUC:",
        round(
            float(
                best_metrics[
                    "roc_auc"
                ]
            ),
            4,
        ),
    )

    # ========================================================
    # SAVE LOCAL MODEL AS BACKUP
    # ========================================================

    local_model_path = (
        MODEL_DIR
        /
        "churn_model.joblib"
    )

    joblib.dump(
        best_pipeline,
        local_model_path,
    )

    print()
    print(
        "Local backup model:",
        local_model_path,
    )

    # ========================================================
    # SAVE MODEL METADATA
    # ========================================================

    metadata = {

        "model_name":
            best_model_name,

        "mlflow_run_id":
            best_run_id,

        "registered_model_name":
            REGISTERED_MODEL_NAME,

        "selection_metric":
            "pr_auc",

        "feature_columns":
            feature_columns,

        "numeric_features":
            numeric_features,

        "categorical_features":
            categorical_features,

        "target":
            "churned",

        "training_rows":
            len(train_df),

        "test_rows":
            len(test_df),

        "training_start_date":
            str(
                train_df[
                    "observation_date"
                ].min()
            ),

        "training_end_date":
            str(
                train_df[
                    "observation_date"
                ].max()
            ),

        "test_start_date":
            str(
                test_df[
                    "observation_date"
                ].min()
            ),

        "test_end_date":
            str(
                test_df[
                    "observation_date"
                ].max()
            ),

        "metrics": {
            key: float(value)
            for (
                key,
                value,
            ) in best_metrics.items()
            if key != "model"
        },
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

    # ========================================================
    # SAVE BEST TEST PREDICTIONS
    # ========================================================

    best_prediction = (
        test_predictions[
            best_model_name
        ][
            "prediction"
        ]
    )

    best_probability = (
        test_predictions[
            best_model_name
        ][
            "probability"
        ]
    )

    prediction_columns = [
        column
        for column in [
            "snapshot_id",
            "customer_id",
            "observation_date",
            "churned",
            "future_orders_60d",
        ]
        if column in test_df.columns
    ]

    prediction_output = (
        test_df[
            prediction_columns
        ]
        .copy()
    )

    prediction_output[
        "churn_probability"
    ] = best_probability

    prediction_output[
        "prediction"
    ] = best_prediction

    prediction_output[
        "model_name"
    ] = best_model_name

    prediction_path = (
        ARTIFACT_DIR
        /
        "churn_test_predictions.csv"
    )

    prediction_output.to_csv(
        prediction_path,
        index=False,
    )

    # ========================================================
    # REGISTER BEST MODEL
    # ========================================================

    print()
    print("=" * 70)
    print("MLFLOW MODEL REGISTRY")
    print("=" * 70)

    model_uri = (
        f"runs:/{best_run_id}/churn_model"
    )

    print(
        "Registering:",
        model_uri,
    )

    registered_model = (
        mlflow.register_model(
            model_uri=model_uri,
            name=REGISTERED_MODEL_NAME,
        )
    )

    model_version = str(
        registered_model.version
    )

    print(
        "Registered model:",
        REGISTERED_MODEL_NAME,
    )

    print(
        "Version:",
        model_version,
    )

    # ========================================================
    # MODEL ALIAS + TAGS
    # ========================================================

    client = MlflowClient()

    client.set_registered_model_alias(
        name=REGISTERED_MODEL_NAME,
        alias="champion",
        version=model_version,
    )

    client.set_model_version_tag(
        name=REGISTERED_MODEL_NAME,
        version=model_version,
        key="project",
        value="RetailPulse AI",
    )

    client.set_model_version_tag(
        name=REGISTERED_MODEL_NAME,
        version=model_version,
        key="problem",
        value="customer_churn",
    )

    client.set_model_version_tag(
        name=REGISTERED_MODEL_NAME,
        version=model_version,
        key="selection_metric",
        value="pr_auc",
    )

    client.set_model_version_tag(
        name=REGISTERED_MODEL_NAME,
        version=model_version,
        key="source_model",
        value=best_model_name,
    )

    # ========================================================
    # VERIFY REGISTRY MODEL
    # ========================================================

    champion_uri = (
        f"models:/"
        f"{REGISTERED_MODEL_NAME}"
        f"@champion"
    )

    print()
    print(
        "Verifying registry model:"
    )

    print(
        champion_uri
    )

    loaded_model = (
        mlflow.sklearn.load_model(
            champion_uri
        )
    )

    # Test a small inference batch.
    sample_size = min(
        5,
        len(X_test),
    )

    sample_predictions = (
        loaded_model.predict(
            X_test.iloc[
                :sample_size
            ]
        )
    )

    sample_probabilities = (
        loaded_model.predict_proba(
            X_test.iloc[
                :sample_size
            ]
        )[:, 1]
    )

    print()
    print(
        "Registry model test predictions:"
    )

    for index in range(
        sample_size
    ):

        print(
            f"  prediction="
            f"{sample_predictions[index]} "
            f"probability="
            f"{sample_probabilities[index]:.4f}"
        )

    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    print()
    print("=" * 70)
    print("SUCCESS")
    print("=" * 70)

    print(
        "Experiment:",
        MLFLOW_EXPERIMENT_NAME,
    )

    print(
        "Selected model:",
        best_model_name,
    )

    print(
        "Run ID:",
        best_run_id,
    )

    print(
        "Registered model:",
        REGISTERED_MODEL_NAME,
    )

    print(
        "Version:",
        model_version,
    )

    print(
        "Alias:",
        "champion",
    )

    print(
        "Registry URI:",
        champion_uri,
    )

    print(
        "Local model:",
        local_model_path,
    )

    print(
        "Metadata:",
        metadata_path,
    )

    print(
        "Comparison:",
        comparison_path,
    )

    print(
        "Predictions:",
        prediction_path,
    )

    print()
    print(
        "RetailPulse churn MLflow "
        "training completed successfully."
    )


if __name__ == "__main__":
    main()
