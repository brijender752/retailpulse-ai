"""Create the namespaces needed to open PyHive sessions and run dbt."""

from iceberg_session import create_iceberg_spark_session


def main():
    spark = create_iceberg_spark_session("RetailPulse - Initialize dbt namespaces")
    try:
        # PyHive selects default while opening a connection, before dbt uses
        # the analytics schema configured in profiles.yml.
        for namespace in ("default", "analytics"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS retailpulse.{namespace}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
