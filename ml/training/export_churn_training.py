from __future__ import annotations

import os
import sys


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)

from iceberg_session import create_iceberg_spark_session


OUTPUT_PATH = (
    "s3a://retailpulse/ml/exports/churn_training"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Export Churn Training"
    )

    try:

        df = spark.sql(
            """
            SELECT *
            FROM retailpulse.ml.churn_training
            ORDER BY observation_date, customer_id
            """
        )

        print("Rows:", df.count())

        df.printSchema()

        (
            df
            .write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )

        print()
        print("SUCCESS")
        print("Exported to:")
        print(OUTPUT_PATH)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()