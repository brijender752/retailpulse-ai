from __future__ import annotations

import sys

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import (
    create_iceberg_spark_session,
)


INPUT_PATH = (
    "s3a://retailpulse/"
    "ml/predictions/churn/"
    "churn_predictions.parquet"
)

TARGET_TABLE = (
    "retailpulse.ml.churn_predictions"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Load Churn Predictions"
    )

    try:

        predictions = (
            spark.read
            .parquet(
                INPUT_PATH
            )
        )

        predictions = (
            predictions
            .withColumn(
                "prediction_date",
                F.to_date(
                    "prediction_date"
                ),
            )
            .withColumn(
                "churn_probability",
                F.col(
                    "churn_probability"
                ).cast("double"),
            )
            .withColumn(
                "revenue_180d",
                F.col(
                    "revenue_180d"
                ).cast("double"),
            )
            .withColumn(
                "revenue_at_risk",
                F.col(
                    "revenue_at_risk"
                ).cast("double"),
            )
        )

        print()
        print(
            "Prediction rows:",
            predictions.count(),
        )

        predictions.printSchema()

        predictions.show(
            20,
            truncate=False,
        )

        spark.sql(
            """
            CREATE NAMESPACE IF NOT EXISTS
            retailpulse.ml
            """
        )

        (
            predictions
            .writeTo(
                TARGET_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "SUCCESS:",
            TARGET_TABLE,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()