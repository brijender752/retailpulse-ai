from datetime import datetime, timezone
import sys
from pyspark.sql import functions as F
sys.path.insert(0, "/opt/retailpulse/lakehouse/iceberg/jobs")
from iceberg_session import create_iceberg_spark_session

MAX_AGE_MINUTES = 30
TABLES = {
    "retailpulse.silver.customers":"_ts_ms",
    "retailpulse.silver.orders":"_ts_ms",
    "retailpulse.silver.payments":"_ts_ms",
}

def main():
    spark = create_iceberg_spark_session("RetailPulse - Freshness")
    failures = []
    try:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        for table, col in TABLES.items():
            if not spark.catalog.tableExists(table):
                warehouse = spark.conf.get("spark.sql.catalog.retailpulse.warehouse")
                failures.append(
                    f"{table}: missing from warehouse {warehouse}. "
                    "Run the retailpulse_iceberg_pipeline DAG to initialize Bronze "
                    "and Silver, and verify quality and spark-iceberg use the same "
                    "ICEBERG_WAREHOUSE and MINIO_S3A_ENDPOINT."
                )
                continue
            df = spark.table(table)
            if col not in df.columns:
                failures.append(f"{table}: missing freshness column {col}")
                continue
            row = df.agg(F.max(col).alias("m")).collect()[0]
            if row["m"] is None:
                failures.append(f"{table}: no timestamp")
                continue
            age = (now_ms - int(row["m"])) / 60000
            print(f"{table}: {age:.2f} minutes old")
            if age > MAX_AGE_MINUTES:
                failures.append(f"{table}: stale ({age:.2f}m)")
        if failures:
            raise RuntimeError("\n".join(failures))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
