# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_timestamp

# from config.settings import (
#     POSTGRES_HOST,
#     POSTGRES_PORT,
#     POSTGRES_DB,
#     POSTGRES_USER,
#     POSTGRES_PASSWORD,
# )


# TABLES = [
#     "customers",
#     "products",
#     "orders",
#     "order_items",
#     "payments",
#     "inventory",
#     "website_events",
#     "support_tickets",
#     "marketing_events",
# ]


# def create_spark_session():

#     return (
#         SparkSession.builder
#         .appName("RetailPulse-Postgres-To-Bronze")
#         .config(
#             "spark.jars",
#             "spark/jars/postgresql-42.7.12.jar",
#         )
#         .getOrCreate()
#     )


# def get_postgres_url():

#     return (
#         f"jdbc:postgresql://"
#         f"{POSTGRES_HOST}:"
#         f"{POSTGRES_PORT}/"
#         f"{POSTGRES_DB}"
#     )


# def read_from_postgres(
#     spark,
#     table_name,
# ):

#     return (
#         spark.read
#         .format("jdbc")
#         .option(
#             "url",
#             get_postgres_url(),
#         )
#         .option(
#             "dbtable",
#             f"ecommerce.{table_name}",
#         )
#         .option(
#             "user",
#             POSTGRES_USER,
#         )
#         .option(
#             "password",
#             POSTGRES_PASSWORD,
#         )
#         .option(
#             "driver",
#             "org.postgresql.Driver",
#         )
#         .load()
#     )


# def write_bronze(
#     df,
#     table_name,
# ):

#     output_path = (
#         f"lakehouse/bronze/{table_name}"
#     )

#     (
#         df
#         .withColumn(
#             "_ingestion_timestamp",
#             current_timestamp(),
#         )
#         .write
#         .mode("overwrite")
#         .parquet(output_path)
#     )


# def main():

#     spark = create_spark_session()

#     spark.sparkContext.setLogLevel("WARN")

#     print("=" * 60)
#     print("RetailPulse AI")
#     print("PostgreSQL → Bronze")
#     print("=" * 60)

#     for table_name in TABLES:

#         print(
#             f"\nReading: ecommerce.{table_name}"
#         )

#         df = read_from_postgres(
#             spark,
#             table_name,
#         )

#         print(
#             f"Rows: {df.count():,}"
#         )

#         print(
#             f"Columns: {len(df.columns)}"
#         )

#         write_bronze(
#             df,
#             table_name,
#         )

#         print(
#             f"Bronze written: "
#             f"lakehouse/bronze/{table_name}"
#         )

#     spark.stop()

#     print("\n" + "=" * 60)
#     print("POSTGRESQL → BRONZE COMPLETED")
#     print("=" * 60)


# if __name__ == "__main__":
#     main()



from pyspark.sql.functions import current_timestamp

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

from spark.configs.spark_session import create_spark_session


# ============================================================
# Configuration
# ============================================================

TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory",
    "website_events",
    "support_tickets",
    "marketing_events",
]

BRONZE_BASE_PATH = "s3a://retailpulse/bronze"


# ============================================================
# PostgreSQL
# ============================================================

def get_postgres_url():
    return (
        f"jdbc:postgresql://"
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/"
        f"{POSTGRES_DB}"
    )


def read_from_postgres(spark, table_name):
    """
    Read a PostgreSQL table using Spark JDBC.
    """

    return (
        spark.read
        .format("jdbc")
        .option("url", get_postgres_url())
        .option("dbtable", f"ecommerce.{table_name}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


# ============================================================
# MinIO Bronze
# ============================================================

def write_bronze(df, table_name):
    """
    Write raw PostgreSQL data to MinIO Bronze layer.
    """

    output_path = f"{BRONZE_BASE_PATH}/{table_name}"

    (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return output_path


# ============================================================
# Main
# ============================================================

def main():

    spark = create_spark_session(
        "RetailPulse-Postgres-To-Bronze"
    )

    print("=" * 70)
    print("RetailPulse AI")
    print("PostgreSQL → MinIO Bronze")
    print("=" * 70)

    try:

        for table_name in TABLES:

            print()
            print("-" * 70)
            print(f"Reading: ecommerce.{table_name}")

            df = read_from_postgres(
                spark,
                table_name
            )

            row_count = df.count()

            print(f"Rows: {row_count:,}")
            print(f"Columns: {len(df.columns)}")

            output_path = write_bronze(
                df,
                table_name
            )

            print(f"Bronze written: {output_path}")

        print()
        print("=" * 70)
        print("POSTGRESQL → MINIO BRONZE COMPLETED")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()