# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     count,
#     sum,
#     avg,
#     max,
#     min,
#     round,
# )


# def create_spark_session():

#     return (
#         SparkSession.builder
#         .appName("RetailPulse-Silver-To-Gold")
#         .getOrCreate()
#     )


# def load_data(spark):

#     customers = spark.read.parquet(
#         "lakehouse/silver/customers"
#     )

#     orders = spark.read.parquet(
#         "lakehouse/silver/orders"
#     )

#     order_items = spark.read.parquet(
#         "lakehouse/silver/order_items"
#     )

#     products = spark.read.parquet(
#         "lakehouse/silver/products"
#     )

#     payments = spark.read.parquet(
#         "lakehouse/silver/payments"
#     )

#     return (
#         customers,
#         orders,
#         order_items,
#         products,
#         payments,
#     )


# def create_customer_360(
#     customers,
#     orders,
# ):

#     order_summary = (
#         orders
#         .groupBy("customer_id")
#         .agg(
#             count("order_id").alias(
#                 "total_orders"
#             ),
#             round(
#                 sum("total_amount"),
#                 2,
#             ).alias(
#                 "total_spend"
#             ),
#             round(
#                 avg("total_amount"),
#                 2,
#             ).alias(
#                 "average_order_value"
#             ),
#             max("order_date").alias(
#                 "last_order_date"
#             ),
#         )
#     )

#     customer_360 = (
#         customers
#         .join(
#             order_summary,
#             on="customer_id",
#             how="left",
#         )
#         .fillna(
#             {
#                 "total_orders": 0,
#                 "total_spend": 0,
#                 "average_order_value": 0,
#             }
#         )
#     )

#     return customer_360


# def create_product_performance(
#     products,
#     order_items,
# ):

#     product_sales = (
#         order_items
#         .groupBy("product_id")
#         .agg(
#             sum("quantity").alias(
#                 "units_sold"
#             ),
#             round(
#                 sum(
#                     col("quantity")
#                     * col("unit_price")
#                 ),
#                 2,
#             ).alias(
#                 "gross_sales"
#             ),
#         )
#     )

#     product_performance = (
#         products
#         .join(
#             product_sales,
#             on="product_id",
#             how="left",
#         )
#         .fillna(
#             {
#                 "units_sold": 0,
#                 "gross_sales": 0,
#             }
#         )
#     )

#     return product_performance


# def create_order_summary(
#     orders,
# ):

#     return (
#         orders
#         .groupBy("status")
#         .agg(
#             count("order_id").alias(
#                 "order_count"
#             ),
#             round(
#                 sum("total_amount"),
#                 2,
#             ).alias(
#                 "total_revenue"
#             ),
#             round(
#                 avg("total_amount"),
#                 2,
#             ).alias(
#                 "average_order_value"
#             ),
#         )
#         .orderBy(
#             col("total_revenue").desc()
#         )
#     )


# def write_gold(
#     df,
#     name,
# ):

#     path = (
#         f"lakehouse/gold/{name}"
#     )

#     (
#         df
#         .write
#         .mode("overwrite")
#         .parquet(path)
#     )

#     print(
#         f"Gold dataset written: {path}"
#     )


# def main():

#     spark = create_spark_session()

#     spark.sparkContext.setLogLevel(
#         "WARN"
#     )

#     print("=" * 60)
#     print("RetailPulse AI")
#     print("Silver → Gold")
#     print("=" * 60)

#     (
#         customers,
#         orders,
#         order_items,
#         products,
#         payments,
#     ) = load_data(spark)

#     print("\nCreating Customer 360...")

#     customer_360 = create_customer_360(
#         customers,
#         orders,
#     )

#     write_gold(
#         customer_360,
#         "customer_360",
#     )

#     print("\nCreating Product Performance...")

#     product_performance = (
#         create_product_performance(
#             products,
#             order_items,
#         )
#     )

#     write_gold(
#         product_performance,
#         "product_performance",
#     )

#     print("\nCreating Order Summary...")

#     order_summary = create_order_summary(
#         orders
#     )

#     write_gold(
#         order_summary,
#         "order_summary",
#     )

#     print("\nCustomer 360 preview:")

#     customer_360.select(
#         "customer_id",
#         "first_name",
#         "last_name",
#         "total_orders",
#         "total_spend",
#     ).orderBy(
#         col("total_spend").desc()
#     ).show(10)

#     print("\nProduct performance preview:")

#     product_performance.select(
#         "product_id",
#         "product_name",
#         "category",
#         "units_sold",
#         "gross_sales",
#     ).orderBy(
#         col("gross_sales").desc()
#     ).show(10)

#     print("\nOrder summary:")

#     order_summary.show()

#     spark.stop()

#     print("\n" + "=" * 60)
#     print("SILVER → GOLD COMPLETED")
#     print("=" * 60)


# if __name__ == "__main__":
#     main() 


from pyspark.sql.functions import (
    sum,
    count,
    avg,
    max,
    col,
)

from spark.configs.spark_session import create_spark_session


# ============================================================
# Paths
# ============================================================

SILVER_BASE_PATH = "s3a://retailpulse/silver"
GOLD_BASE_PATH = "s3a://retailpulse/gold"


# ============================================================
# Helpers
# ============================================================

def read_silver(spark, table_name):

    path = f"{SILVER_BASE_PATH}/{table_name}"

    print(f"Reading Silver: {path}")

    return spark.read.parquet(path)


def write_gold(df, dataset_name):

    path = f"{GOLD_BASE_PATH}/{dataset_name}"

    (
        df
        .write
        .mode("overwrite")
        .parquet(path)
    )

    print(f"Gold written: {path}")


# ============================================================
# Customer 360
# ============================================================

def create_customer_360(spark):

    customers = read_silver(
        spark,
        "customers"
    )

    orders = read_silver(
        spark,
        "orders"
    )

    order_metrics = (
        orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_spend"),
            avg("total_amount").alias("average_order_value"),
            max("order_date").alias("last_order_date"),
        )
    )

    customer_360 = (
        customers
        .join(
            order_metrics,
            on="customer_id",
            how="left"
        )
        .fillna(
            {
                "total_orders": 0,
                "total_spend": 0.0,
                "average_order_value": 0.0,
            }
        )
    )

    return customer_360


# ============================================================
# Product Performance
# ============================================================

def create_product_performance(spark):

    products = read_silver(
        spark,
        "products"
    )

    order_items = read_silver(
        spark,
        "order_items"
    )

    product_metrics = (
        order_items
        .groupBy("product_id")
        .agg(
            sum("quantity").alias("units_sold"),
            sum(
                col("quantity") * col("unit_price")
            ).alias("gross_sales"),
        )
    )

    product_performance = (
        products
        .join(
            product_metrics,
            on="product_id",
            how="left"
        )
        .fillna(
            {
                "units_sold": 0,
                "gross_sales": 0.0,
            }
        )
    )

    return product_performance


# ============================================================
# Order Summary
# ============================================================

def create_order_summary(spark):

    orders = read_silver(
        spark,
        "orders"
    )

    return (
        orders
        .groupBy("status")
        .agg(
            count("order_id").alias("order_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias(
                "average_order_value"
            ),
        )
    )


# ============================================================
# Main
# ============================================================

def main():

    spark = create_spark_session(
        "RetailPulse-Silver-To-Gold"
    )

    print("=" * 70)
    print("RetailPulse AI")
    print("MinIO Silver → MinIO Gold")
    print("=" * 70)

    try:

        # ----------------------------------------------------
        # Customer 360
        # ----------------------------------------------------

        print()
        print("-" * 70)
        print("Creating Customer 360")

        customer_360 = create_customer_360(spark)

        print(
            f"Customer 360 rows: "
            f"{customer_360.count():,}"
        )

        write_gold(
            customer_360,
            "customer_360"
        )

        # ----------------------------------------------------
        # Product Performance
        # ----------------------------------------------------

        print()
        print("-" * 70)
        print("Creating Product Performance")

        product_performance = create_product_performance(
            spark
        )

        print(
            f"Product Performance rows: "
            f"{product_performance.count():,}"
        )

        write_gold(
            product_performance,
            "product_performance"
        )

        # ----------------------------------------------------
        # Order Summary
        # ----------------------------------------------------

        print()
        print("-" * 70)
        print("Creating Order Summary")

        order_summary = create_order_summary(
            spark
        )

        print(
            f"Order Summary rows: "
            f"{order_summary.count():,}"
        )

        write_gold(
            order_summary,
            "order_summary"
        )

        print()
        print("=" * 70)
        print("SILVER → GOLD COMPLETED")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()