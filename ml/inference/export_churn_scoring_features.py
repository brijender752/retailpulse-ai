from __future__ import annotations

import sys


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import (
    create_iceberg_spark_session,
)


SOURCE_TABLE = (
    "retailpulse.ml.churn_scoring_features"
)

OUTPUT_PATH = (
    "s3a://retailpulse/"
    "ml/exports/churn_scoring_features"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Export Churn Scoring Features"
    )

    try:

        df = spark.table(
            SOURCE_TABLE
        )

        count = df.count()

        print(
            "Scoring rows:",
            count,
        )

        if count == 0:
            raise RuntimeError(
                "No scoring rows found."
            )

        (
            df.write
            .mode("overwrite")
            .parquet(
                OUTPUT_PATH
            )
        )

        print()
        print(
            "SUCCESS"
        )

        print(
            OUTPUT_PATH
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()