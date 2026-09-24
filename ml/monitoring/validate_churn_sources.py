from __future__ import annotations

import sys


sys.path.insert(
    0,
    "/opt/retailpulse/lakehouse/iceberg/jobs",
)


from iceberg_session import (
    create_iceberg_spark_session,
)


REQUIRED_TABLES = [
    "retailpulse.analytics.dim_customer",

    "retailpulse.silver.orders",
    "retailpulse.silver.payments",
    "retailpulse.silver.website_events",
    "retailpulse.silver.support_tickets",
    "retailpulse.silver.marketing_events",
]


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse ML - Validate Churn Sources"
    )

    try:

        print()
        print("=" * 70)
        print("VALIDATING ML SOURCES")
        print("=" * 70)

        failures = []

        for table in REQUIRED_TABLES:

            try:

                df = spark.table(
                    table
                )

                count = df.count()

                print(
                    f"{table}: "
                    f"{count:,} rows"
                )

                if count == 0:

                    failures.append(
                        f"{table} is empty"
                    )

            except Exception as exc:

                failures.append(
                    f"{table}: {exc}"
                )

        if failures:

            print()
            print(
                "FAILED:"
            )

            for failure in failures:

                print(
                    " -",
                    failure,
                )

            raise RuntimeError(
                "Required ML sources "
                "are not ready."
            )

        print()
        print(
            "SUCCESS: all ML sources are ready."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()