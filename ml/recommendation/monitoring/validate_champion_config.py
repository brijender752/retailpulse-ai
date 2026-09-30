from __future__ import annotations

import sys


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)

from iceberg_session import create_iceberg_spark_session


TABLE = (
    "retailpulse.ml.recommendation_selected_config"
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse - Validate Recommendation Champion"
    )

    try:

        if not spark.catalog.tableExists(TABLE):

            raise RuntimeError(
                f"Champion config table missing: {TABLE}"
            )

        rows = spark.table(
            TABLE
        ).collect()

        if len(rows) != 1:

            raise RuntimeError(
                "Expected exactly one selected "
                "recommendation configuration. "
                f"Found {len(rows)}."
            )

        row = rows[0]

        personalized = float(
            row["personalized_weight"]
        )

        popularity = float(
            row["popularity_weight"]
        )

        if personalized < 0:

            raise RuntimeError(
                "personalized_weight cannot be negative"
            )

        if popularity < 0:

            raise RuntimeError(
                "popularity_weight cannot be negative"
            )

        if abs(
            (
                personalized
                +
                popularity
            )
            -
            1.0
        ) > 0.000001:

            raise RuntimeError(
                "Recommendation weights must sum to 1.0"
            )

        print()
        print("Champion configuration valid")
        print("Model:", row["model"])
        print(
            "Personalized:",
            personalized,
        )
        print(
            "Popularity:",
            popularity,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()