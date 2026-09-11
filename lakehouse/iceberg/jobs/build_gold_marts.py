
from pyspark.sql import functions as F
from iceberg_session import create_iceberg_spark_session

CATALOG = "retailpulse"

def save(df, table):
    target = f"{CATALOG}.gold.{table}"
    df.writeTo(target).using("iceberg").createOrReplace()
    print(f"{target}: {df.count():,} rows")

def build_order_summary(spark):
    orders = spark.table(f"{CATALOG}.silver.orders")
    payments = spark.table(f"{CATALOG}.silver.payments")

    pay = (
        payments.groupBy("order_id")
        .agg(
            F.sum("amount").alias("payment_amount"),
            F.max("transaction_timestamp").alias("last_payment_timestamp"),
            F.max("payment_status").alias("payment_status"),
        )
    )

    result = (
        orders.alias("o")
        .join(pay.alias("p"), "order_id", "left")
        .select(
            "order_id",
            F.col("o.customer_id").alias("customer_id"),
            "order_date",
            F.col("o.status").alias("order_status"),
            F.col("o.payment_method").alias("payment_method"),
            "shipping_country",
            "shipping_state",
            "total_amount",
            "discount",
            "tax",
            "payment_amount",
            "payment_status",
            "last_payment_timestamp",
        )
        .withColumn(
            "payment_match",
            F.when(F.col("payment_amount").isNull(), F.lit("NO_PAYMENT"))
            .when(
                F.abs(
                    F.coalesce(F.col("payment_amount"), F.lit(0.0))
                    - F.coalesce(F.col("total_amount"), F.lit(0.0))
                ) <= F.lit(0.01),
                F.lit("MATCHED"),
            )
            .otherwise(F.lit("MISMATCH")),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )

    save(result, "order_summary")

def build_product_performance(spark):
    products = spark.table(f"{CATALOG}.silver.products")
    items = spark.table(f"{CATALOG}.silver.order_items")
    orders = spark.table(f"{CATALOG}.silver.orders").select("order_id", "status")

    sales = (
        items.alias("i")
        .join(orders.alias("o"), "order_id", "left")
        .filter(
            ~F.upper(
                F.coalesce(F.col("o.status"), F.lit(""))
            ).isin("CANCELLED", "CANCELED")
        )
        .withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_quantity_sold"),
            F.sum("line_revenue").alias("total_revenue"),
            F.sum(F.coalesce(F.col("discount"), F.lit(0.0))).alias("total_discount"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    result = (
        products.alias("p")
        .join(sales.alias("s"), "product_id", "left")
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            F.col("price").alias("current_price"),
            F.col("inventory_quantity").alias("current_inventory"),
            F.coalesce("total_quantity_sold", F.lit(0)).alias("total_quantity_sold"),
            F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
            F.coalesce("total_discount", F.lit(0.0)).alias("total_discount"),
            F.coalesce("order_count", F.lit(0)).alias("order_count"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )

    save(result, "product_performance")

def build_customer_360(spark):
    customers = spark.table(f"{CATALOG}.silver.customers")
    orders = spark.table(f"{CATALOG}.silver.orders")
    payments = spark.table(f"{CATALOG}.silver.payments")
    website = spark.table(f"{CATALOG}.silver.website_events")
    support = spark.table(f"{CATALOG}.silver.support_tickets")
    marketing = spark.table(f"{CATALOG}.silver.marketing_events")

    om = (
        orders.groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum(F.coalesce(F.col("total_amount"), F.lit(0.0))).alias("total_spend"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    pm = (
        payments.groupBy("customer_id")
        .agg(
            F.sum(F.coalesce(F.col("amount"), F.lit(0.0))).alias("total_payments"),
            F.max("transaction_timestamp").alias("last_payment_date"),
        )
    )

    wm = (
        website.groupBy("customer_id")
        .agg(
            F.count("*").alias("website_event_count"),
            F.max("event_timestamp").alias("last_website_event"),
        )
    )

    sm = support.groupBy("customer_id").agg(
        F.count("*").alias("support_ticket_count")
    )

    mm = (
        marketing.groupBy("customer_id")
        .agg(
            F.sum(F.coalesce(F.col("impression").cast("long"), F.lit(0))).alias("marketing_impressions"),
            F.sum(F.coalesce(F.col("click").cast("long"), F.lit(0))).alias("marketing_clicks"),
            F.sum(F.coalesce(F.col("conversion").cast("long"), F.lit(0))).alias("marketing_conversions"),
        )
    )

    result = (
        customers
        .join(om, "customer_id", "left")
        .join(pm, "customer_id", "left")
        .join(wm, "customer_id", "left")
        .join(sm, "customer_id", "left")
        .join(mm, "customer_id", "left")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "state",
            "city",
            "customer_segment",
            F.coalesce("total_orders", F.lit(0)).alias("total_orders"),
            F.coalesce("total_spend", F.lit(0.0)).alias("total_spend"),
            F.coalesce("total_payments", F.lit(0.0)).alias("total_payments"),
            F.coalesce("website_event_count", F.lit(0)).alias("website_event_count"),
            F.coalesce("support_ticket_count", F.lit(0)).alias("support_ticket_count"),
            F.coalesce("marketing_impressions", F.lit(0)).alias("marketing_impressions"),
            F.coalesce("marketing_clicks", F.lit(0)).alias("marketing_clicks"),
            F.coalesce("marketing_conversions", F.lit(0)).alias("marketing_conversions"),
            "last_order_date",
            "last_payment_date",
            "last_website_event",
        )
        .withColumn(
            "customer_status",
            F.when(F.col("total_orders") > 0, F.lit("ACTIVE"))
            .otherwise(F.lit("PROSPECT")),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )

    save(result, "customer_360")

def main():
    spark = create_iceberg_spark_session(
        "RetailPulse - Build Iceberg Gold Marts"
    )

    try:
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold"
        )

        build_order_summary(spark)
        build_product_performance(spark)
        build_customer_360(spark)

        print("GOLD BUILD SUCCESSFUL")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
