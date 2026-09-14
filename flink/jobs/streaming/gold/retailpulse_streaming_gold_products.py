"""
RetailPulse AI - Streaming Gold
Product Performance

Orders
   +
Order Items
   +
Products
   ↓
PyFlink
   ↓
Stateful Product Aggregation
   ↓
product_performance
   ↓
MinIO / Parquet
"""

import json
from datetime import datetime, timezone

from pyflink.common import Row, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

from pyflink.datastream import (
    StreamExecutionEnvironment,
    KeyedCoProcessFunction,
)

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

from pyflink.table import StreamTableEnvironment

from pyflink.datastream.state import ValueStateDescriptor


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

GOLD_BASE = "s3://retailpulse/streaming/gold"

ORDERS_TOPIC = (
    "retailpulse.ecommerce.orders"
)

ORDER_ITEMS_TOPIC = (
    "retailpulse.ecommerce.order_items"
)

PRODUCTS_TOPIC = (
    "retailpulse.ecommerce.products"
)

GROUP_ORDERS = (
    "retailpulse-streaming-gold-products-orders"
)

GROUP_ORDER_ITEMS = (
    "retailpulse-streaming-gold-products-items"
)

GROUP_PRODUCTS = (
    "retailpulse-streaming-gold-products-products"
)


# ============================================================
# GOLD SCHEMA
# ============================================================

GOLD_FIELDS = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "total_quantity_sold",
    "total_revenue",
    "total_discount",
    "order_count",
    "current_price",
    "current_inventory",
    "_op",
    "_ts_ms",
    "_gold_processed_at",
]


# ============================================================
# GENERIC DEBEZIUM PARSER
# ============================================================

def parse_debezium(message):

    try:

        data = json.loads(message)

        payload = data.get(
            "payload",
            data,
        )

        op = payload.get("op")

        ts_ms = payload.get("ts_ms")

        before = payload.get("before")

        after = payload.get("after")

        if op == "d":

            record = before

        else:

            record = after

        if record is None:

            return None

        if ts_ms is not None:

            try:

                ts_ms = int(ts_ms)

            except (TypeError, ValueError):

                ts_ms = None

        return {
            "record": record,
            "op": op,
            "ts_ms": ts_ms,
        }

    except Exception:

        return None


# ============================================================
# PRODUCT PARSER
# ============================================================

def parse_product(message):

    result = parse_debezium(message)

    if result is None:
        return None

    record = result["record"]

    try:

        product_id = (
            int(record["product_id"])
            if record.get("product_id") is not None
            else None
        )

        if product_id is None:
            return None

        return {
            "product_id": product_id,

            "product_name": record.get(
                "product_name"
            ),

            "category": record.get(
                "category"
            ),

            "brand": record.get(
                "brand"
            ),

            "current_price": (
                float(record["price"])
                if record.get("price") is not None
                else None
            ),

            "current_inventory": (
                int(record["inventory_quantity"])
                if record.get("inventory_quantity") is not None
                else None
            ),

            "op": result["op"],

            "ts_ms": result["ts_ms"],
        }

    except (TypeError, ValueError, KeyError):

        return None


# ============================================================
# ORDER ITEM PARSER
# ============================================================

def parse_order_item(message):

    result = parse_debezium(message)

    if result is None:
        return None

    record = result["record"]

    try:

        product_id = (
            int(record["product_id"])
            if record.get("product_id") is not None
            else None
        )

        order_id = (
            int(record["order_id"])
            if record.get("order_id") is not None
            else None
        )

        if product_id is None:
            return None

        quantity = (
            int(record["quantity"])
            if record.get("quantity") is not None
            else 0
        )

        unit_price = (
            float(record["unit_price"])
            if record.get("unit_price") is not None
            else 0.0
        )

        discount = (
            float(record["discount"])
            if record.get("discount") is not None
            else 0.0
        )

        return {
            "product_id": product_id,

            "order_id": order_id,

            "quantity": quantity,

            "unit_price": unit_price,

            "discount": discount,

            "op": result["op"],

            "ts_ms": result["ts_ms"],
        }

    except (TypeError, ValueError, KeyError):

        return None


# ============================================================
# ORDER PARSER
# ============================================================

def parse_order(message):

    result = parse_debezium(message)

    if result is None:
        return None

    record = result["record"]

    try:

        order_id = (
            int(record["order_id"])
            if record.get("order_id") is not None
            else None
        )

        if order_id is None:
            return None

        return {
            "order_id": order_id,

            "status": record.get(
                "status"
            ),

            "ts_ms": result["ts_ms"],

        }

    except (TypeError, ValueError, KeyError):

        return None


# ============================================================
# PRODUCT PERFORMANCE STATE
# ============================================================

class ProductPerformanceProcessor:

    """
    This processor is intentionally implemented using
    keyed state concepts.

    The product_id is the business key.

    State stores:
        product information
        accumulated quantity
        accumulated revenue
        accumulated discount
        order count
    """

    def __init__(self):

        self.products = {}
        self.metrics = {}

    def process_product(
        self,
        product,
    ):

        if product is None:
            return None

        product_id = product["product_id"]

        if product.get("op") == "d":

            self.products.pop(
                product_id,
                None,
            )

        else:

            self.products[
                product_id
            ] = product

        return self.build_record(
            product_id
        )

    def process_item(
        self,
        item,
    ):

        if item is None:
            return None

        product_id = item["product_id"]

        quantity = item.get(
            "quantity",
            0,
        )

        unit_price = item.get(
            "unit_price",
            0.0,
        )

        discount = item.get(
            "discount",
            0.0,
        )

        if item.get("op") == "d":

            quantity = -quantity
            revenue = -(
                quantity * unit_price
            )
            discount_value = -discount

        else:

            revenue = (
                quantity * unit_price
            )

            discount_value = discount

        if product_id not in self.metrics:

            self.metrics[
                product_id
            ] = {
                "quantity": 0,
                "revenue": 0.0,
                "discount": 0.0,
                "orders": set(),
            }

        metric = self.metrics[
            product_id
        ]

        metric["quantity"] += quantity

        metric["revenue"] += revenue

        metric["discount"] += (
            discount_value
        )

        order_id = item.get(
            "order_id"
        )

        if order_id is not None:

            metric["orders"].add(
                order_id
            )

        return self.build_record(
            product_id
        )

    def process_order(
        self,
        order,
    ):

        return None

    def build_record(
        self,
        product_id,
    ):

        product = self.products.get(
            product_id
        )

        metric = self.metrics.get(
            product_id
        )

        if product is None and metric is None:

            return None

        if metric is None:

            quantity = 0
            revenue = 0.0
            discount = 0.0
            order_count = 0

        else:

            quantity = metric[
                "quantity"
            ]

            revenue = metric[
                "revenue"
            ]

            discount = metric[
                "discount"
            ]

            order_count = len(
                metric["orders"]
            )

        def string_value(value):
            return str(value) if value is not None else None

        return Row(
            product_id,

            string_value(product.get("product_name")) if product else None,

            string_value(product.get("category")) if product else None,

            string_value(product.get("brand")) if product else None,

            quantity,

            revenue,

            discount,

            order_count,

            product.get(
                "current_price"
            ) if product else None,

            product.get(
                "current_inventory"
            ) if product else None,

            string_value(product.get("op")) if product else None,

            product.get(
                "ts_ms"
            ) if product else None,

            datetime.now(
                timezone.utc
            ).isoformat(),
        )


# ============================================================
# KAFKA SOURCE
# ============================================================

def create_kafka_source(
    topic,
    group_id,
):

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP
        )
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )


# ============================================================
# GOLD PARQUET TABLE SINK
# ============================================================

def create_gold_sink(table_env, statement_set, view_name):

    sink_name = "gold_sink_product_performance"

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE `{sink_name}` (
            `product_id` BIGINT,
            `product_name` STRING,
            `category` STRING,
            `brand` STRING,
            `total_quantity_sold` BIGINT,
            `total_revenue` DOUBLE,
            `total_discount` DOUBLE,
            `order_count` BIGINT,
            `current_price` DOUBLE,
            `current_inventory` BIGINT,
            `_op` STRING,
            `_ts_ms` BIGINT,
            `_gold_processed_at` STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{GOLD_BASE}/product_performance',
            'format' = 'parquet'
        )
        """
    )

    statement_set.add_insert_sql(
        f"INSERT INTO `{sink_name}` SELECT * FROM `{view_name}`"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)

    print(
        "RetailPulse AI - Streaming Gold"
    )

    print(
        "Dataset: product_performance"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # Environment
    # --------------------------------------------------------

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    table_env = StreamTableEnvironment.create(env)
    statement_set = table_env.create_statement_set()

    env.set_parallelism(2)

    # --------------------------------------------------------
    # Checkpointing
    # --------------------------------------------------------

    env.enable_checkpointing(
        10_000
    )

    checkpoint_config = (
        env.get_checkpoint_config()
    )

    checkpoint_config.set_checkpoint_timeout(
        120_000
    )

    checkpoint_config.set_min_pause_between_checkpoints(
        30_000
    )

    checkpoint_config.set_max_concurrent_checkpoints(
        1
    )

    # --------------------------------------------------------
    # Product source
    # --------------------------------------------------------

    product_source = create_kafka_source(
        PRODUCTS_TOPIC,
        GROUP_PRODUCTS,
    )

    products = (
        env.from_source(
            product_source,
            WatermarkStrategy.no_watermarks(),
            "Products CDC",
        )
        .map(
            parse_product,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Order item source
    # --------------------------------------------------------

    item_source = create_kafka_source(
        ORDER_ITEMS_TOPIC,
        GROUP_ORDER_ITEMS,
    )

    items = (
        env.from_source(
            item_source,
            WatermarkStrategy.no_watermarks(),
            "Order Items CDC",
        )
        .map(
            parse_order_item,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Orders source
    # --------------------------------------------------------

    order_source = create_kafka_source(
        ORDERS_TOPIC,
        GROUP_ORDERS,
    )

    orders = (
        env.from_source(
            order_source,
            WatermarkStrategy.no_watermarks(),
            "Orders CDC",
        )
        .map(
            parse_order,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # NOTE
    # --------------------------------------------------------
    #
    # Products and order_items are the primary inputs for
    # product performance.
    #
    # Orders are consumed so that the architecture is ready
    # for order-status filtering in the next iteration.
    #
    # --------------------------------------------------------

    processor = (
        ProductPerformanceProcessor()
    )

    # --------------------------------------------------------
    # Product processing
    # --------------------------------------------------------

    product_rows = (
        products
        .map(
            processor.process_product,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Item processing
    # --------------------------------------------------------

    item_rows = (
        items
        .map(
            processor.process_item,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Convert to Gold Row
    # --------------------------------------------------------

    gold_type = Types.ROW_NAMED(
        GOLD_FIELDS,
        [
            Types.LONG(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.LONG(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.LONG(),
            Types.DOUBLE(),
            Types.LONG(),
            Types.STRING(),
            Types.LONG(),
            Types.STRING(),
        ],
    )

    product_gold = (
        product_rows
        .map(
            lambda x: x,
            output_type=gold_type,
        )
    )

    item_gold = (
        item_rows
        .map(
            lambda x: x,
            output_type=gold_type,
        )
    )

    # --------------------------------------------------------
    # Union
    # --------------------------------------------------------

    combined = (
        product_gold
        .union(
            item_gold
        )
    )

    # --------------------------------------------------------
    # Sink
    # --------------------------------------------------------

    view_name = "gold_product_performance_view"
    table_env.create_temporary_view(
        view_name,
        combined,
    )

    create_gold_sink(
        table_env,
        statement_set,
        view_name,
    )

    # --------------------------------------------------------
    # Execute
    # --------------------------------------------------------

    print(
        "Submitting product performance job..."
    )

    statement_set.execute()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()