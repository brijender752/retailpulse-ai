from __future__ import annotations

import sys

from pyspark.sql import functions as F


sys.path.insert(0, "/opt/retailpulse/lakehouse/iceberg/jobs")

from iceberg_session import create_iceberg_spark_session


PREDICTIONS_TABLE = "retailpulse.ml.churn_predictions"


def main():
    spark = create_iceberg_spark_session(
        "RetailPulse ML - Validate Churn Predictions"
    )

    try:
        predictions = spark.table(PREDICTIONS_TABLE)
        required = {
            "customer_id",
            "prediction_date",
            "churn_probability",
            "churn_prediction",
            "risk_band",
            "revenue_at_risk",
        }
        missing = required.difference(predictions.columns)
        if missing:
            raise RuntimeError(
                f"Missing prediction columns: {sorted(missing)}"
            )

        summary = predictions.agg(
            F.count("*").alias("rows"),
            F.sum(
                F.when(
                    F.col("customer_id").isNull()
                    | F.col("prediction_date").isNull()
                    | F.col("churn_probability").isNull()
                    | F.isnan("churn_probability")
                    | (F.col("churn_probability") < 0)
                    | (F.col("churn_probability") > 1)
                    | F.col("churn_prediction").isNull()
                    | ~F.col("churn_prediction").isin(0, 1)
                    | F.col("risk_band").isNull()
                    | ~F.col("risk_band").isin(
                        "LOW", "MEDIUM", "HIGH", "CRITICAL"
                    )
                    | F.col("revenue_at_risk").isNull()
                    | F.isnan("revenue_at_risk")
                    | (F.col("revenue_at_risk") < 0),
                    1,
                ).otherwise(0)
            ).alias("invalid_rows"),
        ).first()

        if summary["rows"] == 0:
            raise RuntimeError("No churn predictions found.")
        if summary["invalid_rows"]:
            raise RuntimeError(
                f"Invalid churn prediction rows: {summary['invalid_rows']}"
            )

        duplicates = (
            predictions.groupBy("customer_id", "prediction_date")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicates:
            raise RuntimeError(
                "Duplicate customer_id and prediction_date values found."
            )

        print(f"SUCCESS: validated {summary['rows']} churn predictions.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
