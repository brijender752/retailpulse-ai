from __future__ import annotations

import sys


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import create_iceberg_spark_session


SOURCE_TABLE = (
    "retailpulse.ml.recommendation_final_test_metrics"
)

OUTPUT_PATH = (
    "s3a://retailpulse/"
    "ml/exports/"
    "recommendation_final_test_metrics"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Export Final Recommendation Metrics"
    )

    try:

        df = spark.table(
            SOURCE_TABLE
        )

        count = df.count()

        if count != 1:

            raise RuntimeError(
                "Expected exactly one final TEST metric row. "
                f"Found {count}."
            )

        print()
        print(
            "Final TEST metric rows:",
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