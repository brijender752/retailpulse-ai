from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    min,
    round,
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName("RetailPulse-Silver-To-Gold")
        .getOrCreate()
    )


def load_data(spark):

    customers = spark.read.parquet(
        "lakehouse/silver/customers"
    )

    orders = spark.read.parquet(
        "lakehouse/silver/orders"
    )

    order_items = spark.read.parquet(
        "lakehouse/silver/order_items"
    )

    products = spark.read.parquet(
        "lakehouse/silver/products"
    )

    payments = spark.read.parquet(
        "lakehouse/silver/payments"
    )

    return (
        customers,
        orders,
        order_items,
        products,
        payments,
    )


def create_customer_360(
    customers,
    orders,
):

    order_summary = (
        orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias(
                "total_orders"
            ),
            round(
                sum("total_amount"),
                2,
            ).alias(
                "total_spend"
            ),
            round(
                avg("total_amount"),
                2,
            ).alias(
                "average_order_value"
            ),
            max("order_date").alias(
                "last_order_date"
            ),
        )
    )

    customer_360 = (
        customers
        .join(
            order_summary,
            on="customer_id",
            how="left",
        )
        .fillna(
            {
                "total_orders": 0,
                "total_spend": 0,
                "average_order_value": 0,
            }
        )
    )

    return customer_360


def create_product_performance(
    products,
    order_items,
):

    product_sales = (
        order_items
        .groupBy("product_id")
        .agg(
            sum("quantity").alias(
                "units_sold"
            ),
            round(
                sum(
                    col("quantity")
                    * col("unit_price")
                ),
                2,
            ).alias(
                "gross_sales"
            ),
        )
    )

    product_performance = (
        products
        .join(
            product_sales,
            on="product_id",
            how="left",
        )
        .fillna(
            {
                "units_sold": 0,
                "gross_sales": 0,
            }
        )
    )

    return product_performance


def create_order_summary(
    orders,
):

    return (
        orders
        .groupBy("status")
        .agg(
            count("order_id").alias(
                "order_count"
            ),
            round(
                sum("total_amount"),
                2,
            ).alias(
                "total_revenue"
            ),
            round(
                avg("total_amount"),
                2,
            ).alias(
                "average_order_value"
            ),
        )
        .orderBy(
            col("total_revenue").desc()
        )
    )


def write_gold(
    df,
    name,
):

    path = (
        f"lakehouse/gold/{name}"
    )

    (
        df
        .write
        .mode("overwrite")
        .parquet(path)
    )

    print(
        f"Gold dataset written: {path}"
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 60)
    print("RetailPulse AI")
    print("Silver → Gold")
    print("=" * 60)

    (
        customers,
        orders,
        order_items,
        products,
        payments,
    ) = load_data(spark)

    print("\nCreating Customer 360...")

    customer_360 = create_customer_360(
        customers,
        orders,
    )

    write_gold(
        customer_360,
        "customer_360",
    )

    print("\nCreating Product Performance...")

    product_performance = (
        create_product_performance(
            products,
            order_items,
        )
    )

    write_gold(
        product_performance,
        "product_performance",
    )

    print("\nCreating Order Summary...")

    order_summary = create_order_summary(
        orders
    )

    write_gold(
        order_summary,
        "order_summary",
    )

    print("\nCustomer 360 preview:")

    customer_360.select(
        "customer_id",
        "first_name",
        "last_name",
        "total_orders",
        "total_spend",
    ).orderBy(
        col("total_spend").desc()
    ).show(10)

    print("\nProduct performance preview:")

    product_performance.select(
        "product_id",
        "product_name",
        "category",
        "units_sold",
        "gross_sales",
    ).orderBy(
        col("gross_sales").desc()
    ).show(10)

    print("\nOrder summary:")

    order_summary.show()

    spark.stop()

    print("\n" + "=" * 60)
    print("SILVER → GOLD COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    main()