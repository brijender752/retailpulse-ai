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
    "retailpulse.ml.recommendation_metrics"
)

OUTPUT_PATH = (
    "s3a://retailpulse/"
    "ml/exports/recommendation_metrics"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Export Recommendation Metrics"
    )

    try:

        print()
        print(
            "Reading:",
            SOURCE_TABLE,
        )

        df = spark.table(
            SOURCE_TABLE
        )

        count = df.count()

        if count == 0:
            raise RuntimeError(
                "Recommendation metrics table is empty."
            )

        print(
            "Rows:",
            count,
        )

        (
            df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                OUTPUT_PATH
            )
        )

        print()
        print(
            "Exported:",
            OUTPUT_PATH,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()