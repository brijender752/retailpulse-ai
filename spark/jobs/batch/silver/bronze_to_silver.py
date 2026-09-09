# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     trim,
#     lower,
#     upper,
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
#         .appName("RetailPulse-Bronze-To-Silver")
#         .getOrCreate()
#     )


# def clean_customers(df):

#     return (
#         df
#         .withColumn(
#             "first_name",
#             trim(col("first_name")),
#         )
#         .withColumn(
#             "last_name",
#             trim(col("last_name")),
#         )
#         .withColumn(
#             "email",
#             lower(trim(col("email"))),
#         )
#         .dropDuplicates(
#             ["customer_id"]
#         )
#         .filter(
#             col("email").isNotNull()
#         )
#     )


# def clean_products(df):

#     return (
#         df
#         .withColumn(
#             "product_name",
#             trim(col("product_name")),
#         )
#         .withColumn(
#             "category",
#             trim(col("category")),
#         )
#         .withColumn(
#             "price",
#             col("price").cast("double"),
#         )
#         .withColumn(
#             "cost",
#             col("cost").cast("double"),
#         )
#         .filter(
#             col("price") >= 0
#         )
#         .dropDuplicates(
#             ["product_id"]
#         )
#     )


# def clean_orders(df):

#     return (
#         df
#         .withColumn(
#             "total_amount",
#             col("total_amount").cast("double"),
#         )
#         .withColumn(
#             "discount",
#             col("discount").cast("double"),
#         )
#         .withColumn(
#             "tax",
#             col("tax").cast("double"),
#         )
#         .filter(
#             col("customer_id").isNotNull()
#         )
#         .dropDuplicates(
#             ["order_id"]
#         )
#     )


# def clean_order_items(df):

#     return (
#         df
#         .withColumn(
#             "quantity",
#             col("quantity").cast("integer"),
#         )
#         .withColumn(
#             "unit_price",
#             col("unit_price").cast("double"),
#         )
#         .withColumn(
#             "discount",
#             col("discount").cast("double"),
#         )
#         .filter(
#             col("quantity") > 0
#         )
#         .dropDuplicates(
#             ["order_item_id"]
#         )
#     )


# def clean_payments(df):

#     return (
#         df
#         .withColumn(
#             "amount",
#             col("amount").cast("double"),
#         )
#         .filter(
#             col("amount") >= 0
#         )
#         .dropDuplicates(
#             ["payment_id"]
#         )
#     )


# def clean_inventory(df):

#     return (
#         df
#         .withColumn(
#             "quantity",
#             col("quantity").cast("integer"),
#         )
#         .withColumn(
#             "reserved_quantity",
#             col("reserved_quantity").cast(
#                 "integer"
#             ),
#         )
#         .filter(
#             col("quantity") >= 0
#         )
#         .dropDuplicates(
#             ["inventory_id"]
#         )
#     )


# def clean_website_events(df):

#     return (
#         df
#         .withColumn(
#             "event_type",
#             upper(col("event_type")),
#         )
#         .dropDuplicates(
#             ["event_id"]
#         )
#     )


# def clean_support_tickets(df):

#     return (
#         df
#         .withColumn(
#             "message",
#             trim(col("message")),
#         )
#         .withColumn(
#             "category",
#             lower(trim(col("category"))),
#         )
#         .dropDuplicates(
#             ["ticket_id"]
#         )
#     )


# def clean_marketing_events(df):

#     return (
#         df
#         .withColumn(
#             "cost",
#             col("cost").cast("double"),
#         )
#         .dropDuplicates(
#             ["marketing_event_id"]
#         )
#     )


# def transform(
#     table_name,
#     df,
# ):

#     transformations = {
#         "customers": clean_customers,
#         "products": clean_products,
#         "orders": clean_orders,
#         "order_items": clean_order_items,
#         "payments": clean_payments,
#         "inventory": clean_inventory,
#         "website_events": clean_website_events,
#         "support_tickets": clean_support_tickets,
#         "marketing_events": clean_marketing_events,
#     }

#     return transformations[
#         table_name
#     ](df)


# def main():

#     spark = create_spark_session()

#     spark.sparkContext.setLogLevel("WARN")

#     print("=" * 60)
#     print("RetailPulse AI")
#     print("Bronze → Silver")
#     print("=" * 60)

#     for table_name in TABLES:

#         print(
#             f"\nProcessing: {table_name}"
#         )

#         input_path = (
#             f"lakehouse/bronze/{table_name}"
#         )

#         output_path = (
#             f"lakehouse/silver/{table_name}"
#         )

#         df = spark.read.parquet(
#             input_path
#         )

#         before_count = df.count()

#         silver_df = transform(
#             table_name,
#             df,
#         )

#         after_count = silver_df.count()

#         (
#             silver_df
#             .write
#             .mode("overwrite")
#             .parquet(output_path)
#         )

#         print(
#             f"Before: {before_count:,}"
#         )

#         print(
#             f"After:  {after_count:,}"
#         )

#         print(
#             f"Written: {output_path}"
#         )

#     spark.stop()

#     print("\n" + "=" * 60)
#     print("BRONZE → SILVER COMPLETED")
#     print("=" * 60)


# if __name__ == "__main__":
#     main()


from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
)

from spark.configs.spark_session import create_spark_session


# ============================================================
# Paths
# ============================================================

BRONZE_BASE_PATH = "s3a://retailpulse/bronze"
SILVER_BASE_PATH = "s3a://retailpulse/silver"


# ============================================================
# Helpers
# ============================================================

def read_bronze(spark, table_name):

    path = f"{BRONZE_BASE_PATH}/{table_name}"

    print(f"Reading Bronze: {path}")

    return spark.read.parquet(path)


def write_silver(df, table_name):

    path = f"{SILVER_BASE_PATH}/{table_name}"

    (
        df
        .write
        .mode("overwrite")
        .parquet(path)
    )

    print(f"Silver written: {path}")


# ============================================================
# Customers
# ============================================================

def transform_customers(df):

    return (
        df
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .filter(col("email").isNotNull())
        .dropDuplicates(["customer_id"])
    )


# ============================================================
# Products
# ============================================================

def transform_products(df):

    return (
        df
        .withColumn(
            "product_name",
            trim(col("product_name"))
        )
        .withColumn(
            "category",
            trim(col("category"))
        )
        .withColumn(
            "subcategory",
            trim(col("subcategory"))
        )
        .withColumn(
            "brand",
            trim(col("brand"))
        )
        .withColumn(
            "price",
            col("price").cast("double")
        )
        .withColumn(
            "cost",
            col("cost").cast("double")
        )
        .filter(col("price") >= 0)
        .dropDuplicates(["product_id"])
    )


# ============================================================
# Orders
# ============================================================

def transform_orders(df):

    return (
        df
        .withColumn(
            "total_amount",
            col("total_amount").cast("double")
        )
        .withColumn(
            "discount",
            col("discount").cast("double")
        )
        .withColumn(
            "tax",
            col("tax").cast("double")
        )
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["order_id"])
    )


# ============================================================
# Order Items
# ============================================================

def transform_order_items(df):

    return (
        df
        .withColumn(
            "quantity",
            col("quantity").cast("int")
        )
        .withColumn(
            "unit_price",
            col("unit_price").cast("double")
        )
        .withColumn(
            "discount",
            col("discount").cast("double")
        )
        .filter(col("quantity") > 0)
        .dropDuplicates(["order_item_id"])
    )


# ============================================================
# Payments
# ============================================================

def transform_payments(df):

    return (
        df
        .withColumn(
            "amount",
            col("amount").cast("double")
        )
        .filter(col("amount") >= 0)
        .dropDuplicates(["payment_id"])
    )


# ============================================================
# Inventory
# ============================================================

def transform_inventory(df):

    return (
        df
        .withColumn(
            "quantity",
            col("quantity").cast("int")
        )
        .withColumn(
            "reserved_quantity",
            col("reserved_quantity").cast("int")
        )
        .filter(col("quantity") >= 0)
        .dropDuplicates(["inventory_id"])
    )


# ============================================================
# Website Events
# ============================================================

def transform_website_events(df):

    return (
        df
        .withColumn(
            "event_type",
            upper(trim(col("event_type")))
        )
        .dropDuplicates(["event_id"])
    )


# ============================================================
# Support Tickets
# ============================================================

def transform_support_tickets(df):

    return (
        df
        .withColumn(
            "message",
            trim(col("message"))
        )
        .withColumn(
            "category",
            lower(trim(col("category")))
        )
        .dropDuplicates(["ticket_id"])
    )


# ============================================================
# Marketing Events
# ============================================================

def transform_marketing_events(df):

    return (
        df
        .withColumn(
            "cost",
            col("cost").cast("double")
        )
        .dropDuplicates(["marketing_event_id"])
    )


# ============================================================
# Main
# ============================================================

def main():

    spark = create_spark_session(
        "RetailPulse-Bronze-To-Silver"
    )

    print("=" * 70)
    print("RetailPulse AI")
    print("MinIO Bronze → MinIO Silver")
    print("=" * 70)

    transformations = {
        "customers": transform_customers,
        "products": transform_products,
        "orders": transform_orders,
        "order_items": transform_order_items,
        "payments": transform_payments,
        "inventory": transform_inventory,
        "website_events": transform_website_events,
        "support_tickets": transform_support_tickets,
        "marketing_events": transform_marketing_events,
    }

    try:

        for table_name, transform_function in transformations.items():

            print()
            print("-" * 70)
            print(f"Processing: {table_name}")

            df = read_bronze(
                spark,
                table_name
            )

            before_count = df.count()

            silver_df = transform_function(df)

            after_count = silver_df.count()

            print(f"Bronze rows: {before_count:,}")
            print(f"Silver rows: {after_count:,}")

            write_silver(
                silver_df,
                table_name
            )

        print()
        print("=" * 70)
        print("BRONZE → SILVER COMPLETED")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()