"""
RetailPulse AI - Streaming Gold
Stateful Order + Payment Enrichment

Orders CDC + Payments CDC
            ↓
        PyFlink
            ↓
       Keyed State
            ↓
     Enriched Orders
            ↓
      MinIO / Parquet

This job demonstrates stateful stream processing.

Orders and payments are keyed by order_id.

The job keeps the latest order/payment information in
keyed state and emits an enriched order record whenever
either side changes.
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

GOLD_BASE = "s3://retailpulse/gold_stream"

GROUP_ID_ORDERS = (
    "retailpulse-streaming-gold-payments-orders"
)

GROUP_ID_PAYMENTS = (
    "retailpulse-streaming-gold-payments-payments"
)


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
# GOLD SCHEMA
# ============================================================

GOLD_FIELDS = [
    "order_id",
    "customer_id",
    "order_date",
    "order_status",
    "payment_method",
    "total_amount",
    "discount",
    "tax",
    "payment_amount",
    "payment_status",
    "transaction_timestamp",
    "payment_match",
    "_op",
    "_ts_ms",
    "_gold_processed_at",
]


# ============================================================
# DEBEZIUM PARSER
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
            "customer_id": (
                int(record["customer_id"])
                if record.get("customer_id") is not None
                else None
            ),
            "order_date": record.get(
                "order_date"
            ),
            "order_status": record.get(
                "status"
            ),
            "payment_method": record.get(
                "payment_method"
            ),
            "total_amount": (
                float(record["total_amount"])
                if record.get("total_amount") is not None
                else None
            ),
            "discount": (
                float(record["discount"])
                if record.get("discount") is not None
                else None
            ),
            "tax": (
                float(record["tax"])
                if record.get("tax") is not None
                else None
            ),
            "op": result["op"],
            "ts_ms": (
                int(result["ts_ms"])
                if result["ts_ms"] is not None
                else None
            ),
        }

    except (TypeError, ValueError, KeyError):

        return None


# ============================================================
# PAYMENT PARSER
# ============================================================

def parse_payment(message):

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
            "payment_amount": (
                float(record["amount"])
                if record.get("amount") is not None
                else None
            ),
            "payment_status": record.get(
                "payment_status"
            ),
            "transaction_timestamp": record.get(
                "transaction_timestamp"
            ),
            "op": result["op"],
            "ts_ms": (
                int(result["ts_ms"])
                if result["ts_ms"] is not None
                else None
            ),
        }

    except (TypeError, ValueError, KeyError):

        return None


# ============================================================
# STATEFUL ORDER + PAYMENT PROCESSOR
# ============================================================

class OrderPaymentProcessor(
    KeyedCoProcessFunction
):

    def open(self, runtime_context):

        order_descriptor = (
            ValueStateDescriptor(
                "latest_order",
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        payment_descriptor = (
            ValueStateDescriptor(
                "latest_payment",
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        self.order_state = (
            runtime_context.get_state(
                order_descriptor
            )
        )

        self.payment_state = (
            runtime_context.get_state(
                payment_descriptor
            )
        )

    # --------------------------------------------------------
    # Order stream
    # --------------------------------------------------------

    def process_element1(
        self,
        order,
        ctx,
    ):

        if order is None:

            return

        op = order.get("op")

        # ----------------------------------------------------
        # Delete order
        # ----------------------------------------------------

        if op == "d":

            self.order_state.clear()

        else:

            self.order_state.update(
                order
            )

        enriched = self.emit_enriched()
        if enriched is not None:
            yield enriched

    # --------------------------------------------------------
    # Payment stream
    # --------------------------------------------------------

    def process_element2(
        self,
        payment,
        ctx,
    ):

        if payment is None:

            return

        op = payment.get("op")

        # ----------------------------------------------------
        # Delete payment
        # ----------------------------------------------------

        if op == "d":

            self.payment_state.clear()

        else:

            self.payment_state.update(
                payment
            )

        enriched = self.emit_enriched()
        if enriched is not None:
            yield enriched

    # --------------------------------------------------------
    # Build enriched order
    # --------------------------------------------------------

    def emit_enriched(
        self,
    ):

        order = self.order_state.value()

        payment = self.payment_state.value()

        # We need an order before creating
        # an order-level Gold record.

        if order is None:

            return None

        payment_exists = (
            payment is not None
        )

        payment_amount = None
        payment_status = None
        transaction_timestamp = None

        if payment_exists:

            payment_amount = payment.get(
                "payment_amount"
            )

            payment_status = payment.get(
                "payment_status"
            )

            transaction_timestamp = (
                payment.get(
                    "transaction_timestamp"
                )
            )

        # ----------------------------------------------------
        # Determine latest CDC metadata
        # ----------------------------------------------------

        op = order.get("op")

        ts_ms = order.get("ts_ms")

        if payment_exists:

            payment_ts = payment.get(
                "ts_ms"
            )

            if (
                payment_ts is not None
                and (
                    ts_ms is None
                    or payment_ts > ts_ms
                )
            ):

                ts_ms = payment_ts

                op = payment.get(
                    "op"
                )

        # ----------------------------------------------------
        # Output
        # ----------------------------------------------------

        def string_value(value):
            return str(value) if value is not None else None

        return Row(
            order.get("order_id"),
            order.get("customer_id"),
            string_value(order.get("order_date")),
            string_value(order.get("order_status")),
            string_value(order.get("payment_method")),
            order.get("total_amount"),
            order.get("discount"),
            order.get("tax"),
            payment_amount,
            string_value(payment_status),
            string_value(transaction_timestamp),
            payment_exists,
            string_value(op),
            ts_ms,
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

    sink_name = "gold_sink_order_payment_summary"

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE `{sink_name}` (
            `order_id` BIGINT,
            `customer_id` BIGINT,
            `order_date` STRING,
            `order_status` STRING,
            `payment_method` STRING,
            `total_amount` DOUBLE,
            `discount` DOUBLE,
            `tax` DOUBLE,
            `payment_amount` DOUBLE,
            `payment_status` STRING,
            `transaction_timestamp` STRING,
            `payment_match` BOOLEAN,
            `_op` STRING,
            `_ts_ms` BIGINT,
            `_gold_processed_at` STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{GOLD_BASE}/order_payment_summary',
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
        "RetailPulse AI - Stateful Streaming Gold"
    )

    print(
        "Dataset: order_payment_summary"
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
    # Orders source
    # --------------------------------------------------------

    orders_source = create_kafka_source(
        ORDERS_TOPIC,
        GROUP_ID_ORDERS,
    )

    orders_stream = (
        env.from_source(
            orders_source,
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
    # Payments source
    # --------------------------------------------------------

    payments_source = create_kafka_source(
        PAYMENTS_TOPIC,
        GROUP_ID_PAYMENTS,
    )

    payments_stream = (
        env.from_source(
            payments_source,
            WatermarkStrategy.no_watermarks(),
            "Payments CDC",
        )
        .map(
            parse_payment,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Key both streams by order_id
    # --------------------------------------------------------

    keyed_orders = (
        orders_stream
        .key_by(
            lambda x: x["order_id"],
            key_type=Types.LONG(),
        )
    )

    keyed_payments = (
        payments_stream
        .key_by(
            lambda x: x["order_id"],
            key_type=Types.LONG(),
        )
    )

    # --------------------------------------------------------
    # Connect streams
    # --------------------------------------------------------

    connected = (
        keyed_orders
        .connect(keyed_payments)
    )

    # --------------------------------------------------------
    # Stateful processing
    # --------------------------------------------------------

    processor = OrderPaymentProcessor()

    enriched = (
        connected
        .process(
            processor,
            output_type=Types.ROW_NAMED(
                GOLD_FIELDS,
                [
                    Types.LONG(),
                    Types.LONG(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.BOOLEAN(),
                    Types.STRING(),
                    Types.LONG(),
                    Types.STRING(),
                ],
            ),
        )
    )

    # --------------------------------------------------------
    # Sink
    # --------------------------------------------------------

    view_name = "gold_order_payment_summary_view"
    table_env.create_temporary_view(
        view_name,
        enriched,
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
        "Submitting stateful Streaming Gold job..."
    )

    statement_set.execute()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()