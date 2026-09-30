from __future__ import annotations

import sys

from pyspark.sql import functions as F


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import create_iceberg_spark_session


EVENTS_TABLE = (
    "retailpulse.ml.customer_product_events"
)

OUTPUT_TABLE = (
    "retailpulse.ml.recommendation_temporal_split"
)

VALIDATION_DAYS = 30
TEST_DAYS = 30


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Recommendation Temporal Split"
    )

    try:

        events = spark.table(
            EVENTS_TABLE
        )

        max_date = (
            events
            .agg(
                F.max(
                    "event_date"
                ).alias("max_date")
            )
            .first()["max_date"]
        )

        if max_date is None:

            raise RuntimeError(
                "No event dates found."
            )

        boundaries = (
            spark.createDataFrame(
                [(max_date,)],
                ["max_date"],
            )
            .select(
                "max_date",

                F.date_sub(
                    "max_date",
                    TEST_DAYS,
                ).alias(
                    "test_start"
                ),

                F.date_sub(
                    "max_date",
                    TEST_DAYS
                    +
                    VALIDATION_DAYS,
                ).alias(
                    "validation_start"
                ),
            )
            .first()
        )

        validation_start = (
            boundaries[
                "validation_start"
            ]
        )

        test_start = (
            boundaries[
                "test_start"
            ]
        )

        print()
        print("=" * 70)
        print("TEMPORAL SPLIT")
        print("=" * 70)

        print(
            "Dataset end:",
            max_date,
        )

        print(
            "Validation start:",
            validation_start,
        )

        print(
            "Test start:",
            test_start,
        )

        split = (
            events
            .withColumn(
                "split",

                F.when(
                    F.col("event_date")
                    <
                    F.lit(
                        validation_start
                    ),

                    F.lit("TRAIN"),

                ).when(
                    F.col("event_date")
                    <
                    F.lit(
                        test_start
                    ),

                    F.lit("VALIDATION"),

                ).otherwise(
                    F.lit("TEST")
                ),
            )
            .withColumn(
                "validation_start",
                F.lit(
                    validation_start
                ),
            )
            .withColumn(
                "test_start",
                F.lit(
                    test_start
                ),
            )
        )

        print()

        stats = (
            split
            .groupBy("split")
            .agg(
                F.count("*").alias(
                    "events"
                ),

                F.countDistinct(
                    "customer_id"
                ).alias(
                    "customers"
                ),

                F.countDistinct(
                    "product_id"
                ).alias(
                    "products"
                ),

                F.min(
                    "event_date"
                ).alias(
                    "min_date"
                ),

                F.max(
                    "event_date"
                ).alias(
                    "max_date"
                ),
            )
        )

        stats.show(
            truncate=False
        )

        split_names = {
            row["split"]
            for row in stats.collect()
        }

        required = {
            "TRAIN",
            "VALIDATION",
            "TEST",
        }

        missing = (
            required
            -
            split_names
        )

        if missing:

            raise RuntimeError(
                "Missing temporal splits: "
                + ", ".join(
                    sorted(missing)
                )
                +
                ". Increase synthetic history or "
                "reduce split windows."
            )

        (
            split
            .writeTo(
                OUTPUT_TABLE
            )
            .using("iceberg")
            .createOrReplace()
        )

        print()
        print(
            "SUCCESS:",
            OUTPUT_TABLE,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()