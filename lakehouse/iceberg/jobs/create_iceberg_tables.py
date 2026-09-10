from iceberg_session import (
    create_iceberg_spark_session,
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulse - Create Iceberg Lakehouse"
    )

    print(
        "Creating RetailPulse Iceberg namespaces..."
    )

    spark.sql(
        """
        CREATE NAMESPACE IF NOT EXISTS
        retailpulse.bronze
        """
    )

    spark.sql(
        """
        CREATE NAMESPACE IF NOT EXISTS
        retailpulse.silver
        """
    )

    spark.sql(
        """
        CREATE NAMESPACE IF NOT EXISTS
        retailpulse.gold
        """
    )

    print("Namespaces:")

    spark.sql(
        """
        SHOW NAMESPACES
        IN retailpulse
        """
    ).show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()