from iceberg_session import create_iceberg_spark_session

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Create Iceberg Namespaces"
    )
    try:
        for namespace in ("bronze", "silver", "gold"):
            spark.sql(
                f"CREATE NAMESPACE IF NOT EXISTS retailpulse.{namespace}"
            )
        spark.sql("SHOW NAMESPACES IN retailpulse").show(truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
