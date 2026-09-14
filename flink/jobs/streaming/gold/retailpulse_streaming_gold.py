"""
RetailPulse AI - Streaming Gold
Order Summary

Kafka CDC
    ↓
Streaming Silver
    ↓
PyFlink
    ↓
Streaming Gold
    ↓
MinIO / Parquet

First Streaming Gold dataset:
    order_summary

This first version consumes the orders and payments
CDC streams and produces an enriched order-level stream.
"""

import json
from datetime import datetime, timezone

from pyflink.common import Row, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

from pyflink.datastream import StreamExecutionEnvironment

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

from pyflink.table import StreamTableEnvironment


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

GOLD_BASE = "s3://retailpulse/streaming/gold"

GROUP_ID = "retailpulse-streaming-gold-order-summary"


# ============================================================
# TOPICS
# ============================================================

ORDERS_TOPIC = (
    "retailpulse.ecommerce.orders"
)

PAYMENTS_TOPIC = (
    "retailpulse.ecommerce.payments"
)


# ============================================================
# ORDER GOLD SCHEMA
# ============================================================

ORDER_GOLD_FIELDS = [
    "order_id",
    "customer_id",
    "order_date",
    "status",
    "payment_method",
    "shipping_country",
    "shipping_state",
    "total_amount",
    "discount",
    "tax",
    "_op",
    "_ts_ms",
    "_source_table",
    "_gold_processed_at",
]


# ============================================================
# PARSE DEBEZIUM
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

        return {
            "record": record,
            "op": op,
            "ts_ms": ts_ms,
        }

    except Exception:

        return None


# ============================================================
# ORDER TRANSFORM
# ============================================================

def transform_order(data):

    if data is None:

        return None

    record = data["record"]

    op = data["op"]

    ts_ms = data["ts_ms"]

    try:

        def string_value(field_name):
            value = record.get(field_name)
            return str(value) if value is not None else None

        order_id = (
            int(record["order_id"])
            if record.get("order_id") is not None
            else None
        )

        customer_id = (
            int(record["customer_id"])
            if record.get("customer_id") is not None
            else None
        )

        total_amount = (
            float(record["total_amount"])
            if record.get("total_amount") is not None
            else None
        )

        discount = (
            float(record["discount"])
            if record.get("discount") is not None
            else None
        )

        tax = (
            float(record["tax"])
            if record.get("tax") is not None
            else None
        )

        if ts_ms is not None:

            try:
                ts_ms = int(ts_ms)

            except (TypeError, ValueError):

                ts_ms = None

        return Row(
            order_id,
            customer_id,
            string_value("order_date"),
            string_value("status"),
            string_value("payment_method"),
            string_value("shipping_country"),
            string_value("shipping_state"),
            total_amount,
            discount,
            tax,
            str(op) if op is not None else None,
            ts_ms,
            "orders",
            datetime.now(
                timezone.utc
            ).isoformat(),
        )

    except Exception:

        return None


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

    output_path = (
        f"{GOLD_BASE}/order_summary"
    )

    sink_name = "gold_sink_order_summary"

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE `{sink_name}` (
            `order_id` BIGINT,
            `customer_id` BIGINT,
            `order_date` STRING,
            `status` STRING,
            `payment_method` STRING,
            `shipping_country` STRING,
            `shipping_state` STRING,
            `total_amount` DOUBLE,
            `discount` DOUBLE,
            `tax` DOUBLE,
            `_op` STRING,
            `_ts_ms` BIGINT,
            `_source_table` STRING,
            `_gold_processed_at` STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{output_path}',
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
        "Dataset: order_summary"
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
    # Orders Kafka source
    # --------------------------------------------------------

    orders_source = create_kafka_source(
        ORDERS_TOPIC,
        GROUP_ID,
    )

    orders_stream = (
        env.from_source(
            orders_source,
            WatermarkStrategy.no_watermarks(),
            "Streaming Silver Orders",
        )
    )

    # --------------------------------------------------------
    # Parse Debezium
    # --------------------------------------------------------

    parsed_orders = (
        orders_stream
        .map(
            parse_debezium,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    # --------------------------------------------------------
    # Transform to Gold
    # --------------------------------------------------------

    order_type = Types.ROW_NAMED(
        ORDER_GOLD_FIELDS,
        [
            Types.LONG(),
            Types.LONG(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.LONG(),
            Types.STRING(),
            Types.STRING(),
        ],
    )

    gold_orders = (
        parsed_orders
        .map(
            transform_order,
            output_type=order_type,
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Gold sink
    # --------------------------------------------------------

    view_name = "gold_order_summary_view"
    table_env.create_temporary_view(
        view_name,
        gold_orders,
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
        "Submitting Streaming Gold job..."
    )

    statement_set.execute()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()