"""Initialize missing Bronze Iceberg tables without replacing existing tables."""
from bootstrap_minio_to_iceberg import DATASETS, migrate_one
from iceberg_session import create_iceberg_spark_session


def main():
    spark = create_iceberg_spark_session("RetailPulse - Initialize Streaming Bronze")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS retailpulse.bronze")
        for dataset in DATASETS:
            if dataset.target.startswith("retailpulse.bronze."):
                if spark.catalog.tableExists(dataset.target):
                    print(f"Retaining existing {dataset.target}")
                else:
                    migrate_one(spark, dataset)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
