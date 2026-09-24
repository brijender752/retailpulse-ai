from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import (
    create_iceberg_spark_session,
)


PREDICTIONS_TABLE = (
    "retailpulse.ml.churn_predictions"
)

SCORING_TABLE = (
    "retailpulse.ml.churn_scoring_features"
)

MONITORING_TABLE = (
    "retailpulse.ml.churn_monitoring"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Churn Monitoring"
    )

    try:

        # ====================================================
        # LOAD DATA
        # ====================================================

        predictions = spark.table(
            PREDICTIONS_TABLE
        )

        features = spark.table(
            SCORING_TABLE
        )

        prediction_count = (
            predictions.count()
        )

        feature_count = (
            features.count()
        )

        if prediction_count == 0:
            raise RuntimeError(
                "No churn predictions found."
            )

        if feature_count == 0:
            raise RuntimeError(
                "No scoring features found."
            )

        # ====================================================
        # BASIC COVERAGE
        # ====================================================

        coverage = (
            prediction_count
            /
            feature_count
        )

        # ====================================================
        # PREDICTION STATISTICS
        # ====================================================

        stats = (
            predictions
            .agg(

                F.avg(
                    "churn_probability"
                ).alias(
                    "avg_probability"
                ),

                F.min(
                    "churn_probability"
                ).alias(
                    "min_probability"
                ),

                F.max(
                    "churn_probability"
                ).alias(
                    "max_probability"
                ),

                F.avg(
                    F.col(
                        "churn_prediction"
                    ).cast("double")
                ).alias(
                    "predicted_churn_rate"
                ),

                F.sum(
                    "revenue_at_risk"
                ).alias(
                    "total_revenue_at_risk"
                ),
            )
            .first()
        )

        avg_probability = float(
            stats[
                "avg_probability"
            ]
            or 0.0
        )

        min_probability = float(
            stats[
                "min_probability"
            ]
            or 0.0
        )

        max_probability = float(
            stats[
                "max_probability"
            ]
            or 0.0
        )

        predicted_churn_rate = float(
            stats[
                "predicted_churn_rate"
            ]
            or 0.0
        )

        revenue_at_risk = float(
            stats[
                "total_revenue_at_risk"
            ]
            or 0.0
        )

        # ====================================================
        # INVALID PROBABILITIES
        # ====================================================

        invalid_probability_count = (
            predictions
            .filter(
                F.col(
                    "churn_probability"
                ).isNull()
                |
                (
                    F.col(
                        "churn_probability"
                    )
                    < 0
                )
                |
                (
                    F.col(
                        "churn_probability"
                    )
                    > 1
                )
            )
            .count()
        )

        # ====================================================
        # DUPLICATE CUSTOMERS
        # ====================================================

        duplicate_customers = (
            predictions
            .groupBy(
                "customer_id"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        # ====================================================
        # RISK BAND DISTRIBUTION
        # ====================================================

        risk_rows = (
            predictions
            .groupBy(
                "risk_band"
            )
            .count()
            .collect()
        )

        risk_distribution = {
            row[
                "risk_band"
            ]: int(
                row["count"]
            )
            for row in risk_rows
        }

        low_count = int(
            risk_distribution.get(
                "LOW",
                0,
            )
        )

        medium_count = int(
            risk_distribution.get(
                "MEDIUM",
                0,
            )
        )

        high_count = int(
            risk_distribution.get(
                "HIGH",
                0,
            )
        )

        critical_count = int(
            risk_distribution.get(
                "CRITICAL",
                0,
            )
        )

        # ====================================================
        # MODEL VERSION
        # ====================================================

        model_versions = (
            predictions
            .select(
                "model_name",
                "model_version",
                "model_alias",
            )
            .distinct()
            .collect()
        )

        if len(model_versions) != 1:

            raise RuntimeError(
                "Prediction table contains "
                "multiple model versions."
            )

        model_row = (
            model_versions[0]
        )

        model_name = str(
            model_row[
                "model_name"
            ]
        )

        model_version = str(
            model_row[
                "model_version"
            ]
        )

        model_alias = str(
            model_row[
                "model_alias"
            ]
        )

        # ====================================================
        # VALIDATION STATUS
        # ====================================================

        errors = []

        if coverage < 0.99:

            errors.append(
                "Prediction coverage below 99%."
            )

        if invalid_probability_count > 0:

            errors.append(
                "Invalid churn probabilities found."
            )

        if duplicate_customers > 0:

            errors.append(
                "Duplicate customer predictions found."
            )

        if not (
            0.0
            <=
            avg_probability
            <=
            1.0
        ):

            errors.append(
                "Average probability is invalid."
            )

        status = (
            "PASS"
            if not errors
            else "FAIL"
        )

        # ====================================================
        # CREATE MONITORING ROW
        # ====================================================

        monitored_at = (
            datetime.now(
                timezone.utc
            )
        )

        monitoring_data = [
            (
                monitored_at,
                status,
                prediction_count,
                feature_count,
                float(coverage),
                avg_probability,
                min_probability,
                max_probability,
                predicted_churn_rate,
                revenue_at_risk,
                low_count,
                medium_count,
                high_count,
                critical_count,
                invalid_probability_count,
                duplicate_customers,
                model_name,
                model_version,
                model_alias,
                json.dumps(
                    errors
                ),
            )
        ]

        schema = """
            monitored_at timestamp,
            status string,
            prediction_count long,
            feature_count long,
            prediction_coverage double,
            avg_churn_probability double,
            min_churn_probability double,
            max_churn_probability double,
            predicted_churn_rate double,
            revenue_at_risk double,
            low_risk_customers long,
            medium_risk_customers long,
            high_risk_customers long,
            critical_risk_customers long,
            invalid_probability_count long,
            duplicate_customer_count long,
            model_name string,
            model_version string,
            model_alias string,
            validation_errors string
        """

        monitoring_df = (
            spark.createDataFrame(
                monitoring_data,
                schema=schema,
            )
        )

        # ====================================================
        # WRITE HISTORY
        # ====================================================

        if not spark.catalog.tableExists(
            MONITORING_TABLE
        ):

            (
                monitoring_df
                .writeTo(
                    MONITORING_TABLE
                )
                .using("iceberg")
                .create()
            )

        else:

            (
                monitoring_df
                .writeTo(
                    MONITORING_TABLE
                )
                .append()
            )

        # ====================================================
        # OUTPUT
        # ====================================================

        print()
        print("=" * 70)
        print("CHURN MONITORING")
        print("=" * 70)

        print(
            "Status:",
            status,
        )

        print(
            "Predictions:",
            prediction_count,
        )

        print(
            "Features:",
            feature_count,
        )

        print(
            "Coverage:",
            round(
                coverage,
                4,
            ),
        )

        print(
            "Average probability:",
            round(
                avg_probability,
                4,
            ),
        )

        print(
            "Predicted churn rate:",
            round(
                predicted_churn_rate,
                4,
            ),
        )

        print(
            "Revenue at risk:",
            round(
                revenue_at_risk,
                2,
            ),
        )

        print()
        print(
            "Risk distribution:"
        )

        print(
            risk_distribution
        )

        print()
        print(
            "Model:",
            model_name,
        )

        print(
            "Version:",
            model_version,
        )

        print(
            "Alias:",
            model_alias,
        )

        if errors:

            print()
            print(
                "Validation errors:"
            )

            for error in errors:
                print(
                    " -",
                    error,
                )

            raise RuntimeError(
                "Churn monitoring failed: "
                +
                "; ".join(
                    errors
                )
            )

        print()
        print(
            "SUCCESS: churn monitoring passed."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()