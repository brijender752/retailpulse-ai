import json
from datetime import datetime

from pyflink.common import Row, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    OnCheckpointRollingPolicy,
    RollingPolicy,
)
from pyflink.datastream.formats.parquet import ParquetBulkWriters
from pyflink.common import WatermarkStrategy
from pyflink.table import DataTypes


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"
MINIO_BASE = "s3://retailpulse/bronze"

KAFKA_TOPICS = {
    "customers": "retailpulse.ecommerce.customers",
    "products": "retailpulse.ecommerce.products",
    "orders": "retailpulse.ecommerce.orders",
    "order_items": "retailpulse.ecommerce.order_items",
    "payments": "retailpulse.ecommerce.payments",
    "inventory": "retailpulse.ecommerce.inventory",
    "website_events": "retailpulse.ecommerce.website_events",
    "support_tickets": "retailpulse.ecommerce.support_tickets",
    "marketing_events": "retailpulse.ecommerce.marketing_events",
}


# ============================================================
# Debezium CDC parser
# ============================================================

def parse_debezium(message):
    """
    Convert a Debezium CDC message into:

        Row(
            op,
            ts_ms,
            before,
            after
        )

    op:
        r = snapshot/read
        c = create
        u = update
        d = delete
    """

    try:
        data = json.loads(message)

        payload = data.get("payload", data)

        op = payload.get("op")

        if op is None:
            return None

        return Row(
            op=op,
            ts_ms=payload.get("ts_ms"),
            before=payload.get("before"),
            after=payload.get("after"),
        )

    except Exception as exc:
        print(f"CDC parse error: {exc}")
        return None


# ============================================================
# PostgreSQL schemas
# ============================================================

SCHEMAS = {

    "customers": DataTypes.ROW([
        DataTypes.FIELD("customer_id", DataTypes.BIGINT()),
        DataTypes.FIELD("first_name", DataTypes.STRING()),
        DataTypes.FIELD("last_name", DataTypes.STRING()),
        DataTypes.FIELD("email", DataTypes.STRING()),
        DataTypes.FIELD("phone", DataTypes.STRING()),
        DataTypes.FIELD("country", DataTypes.STRING()),
        DataTypes.FIELD("state", DataTypes.STRING()),
        DataTypes.FIELD("city", DataTypes.STRING()),
        DataTypes.FIELD("signup_date", DataTypes.STRING()),
        DataTypes.FIELD("customer_segment", DataTypes.STRING()),
        DataTypes.FIELD("updated_at", DataTypes.STRING()),
        DataTypes.FIELD("created_at", DataTypes.STRING()),
    ]),

    "products": DataTypes.ROW([
        DataTypes.FIELD("product_id", DataTypes.BIGINT()),
        DataTypes.FIELD("product_name", DataTypes.STRING()),
        DataTypes.FIELD("category", DataTypes.STRING()),
        DataTypes.FIELD("subcategory", DataTypes.STRING()),
        DataTypes.FIELD("brand", DataTypes.STRING()),
        DataTypes.FIELD("price", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD("cost", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD("supplier_id", DataTypes.BIGINT()),
        DataTypes.FIELD("inventory_quantity", DataTypes.INT()),
        DataTypes.FIELD("created_at", DataTypes.STRING()),
        DataTypes.FIELD("updated_at", DataTypes.STRING()),
    ]),

    "orders": DataTypes.ROW([
        DataTypes.FIELD("order_id", DataTypes.BIGINT()),
        DataTypes.FIELD("customer_id", DataTypes.BIGINT()),
        DataTypes.FIELD("order_date", DataTypes.STRING()),
        DataTypes.FIELD("status", DataTypes.STRING()),
        DataTypes.FIELD("payment_method", DataTypes.STRING()),
        DataTypes.FIELD("shipping_country", DataTypes.STRING()),
        DataTypes.FIELD("shipping_state", DataTypes.STRING()),
        DataTypes.FIELD("total_amount", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD("discount", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD("tax", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD("created_at", DataTypes.STRING()),
        DataTypes.FIELD("updated_at", DataTypes.STRING()),
    ]),

    "order_items": DataTypes.ROW([
        DataTypes.FIELD("order_item_id", DataTypes.BIGINT()),
        DataTypes.FIELD("order_id", DataTypes.BIGINT()),
        DataTypes.FIELD("product_id", DataTypes.BIGINT()),
        DataTypes.FIELD("quantity", DataTypes.INT()),
        DataTypes.FIELD("unit_price", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD("discount", DataTypes.DECIMAL(12, 2)),
    ]),

    "payments": DataTypes.ROW([
        DataTypes.FIELD("payment_id", DataTypes.BIGINT()),
        DataTypes.FIELD("order_id", DataTypes.BIGINT()),
        DataTypes.FIELD("customer_id", DataTypes.BIGINT()),
        DataTypes.FIELD("amount", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD("payment_method", DataTypes.STRING()),
        DataTypes.FIELD("payment_status", DataTypes.STRING()),
        DataTypes.FIELD(
            "transaction_timestamp",
            DataTypes.STRING(),
        ),
    ]),

    "inventory": DataTypes.ROW([
        DataTypes.FIELD("inventory_id", DataTypes.BIGINT()),
        DataTypes.FIELD("product_id", DataTypes.BIGINT()),
        DataTypes.FIELD("warehouse_id", DataTypes.BIGINT()),
        DataTypes.FIELD("quantity", DataTypes.INT()),
        DataTypes.FIELD("reserved_quantity", DataTypes.INT()),
        DataTypes.FIELD("updated_at", DataTypes.STRING()),
    ]),

    "website_events": DataTypes.ROW([
        DataTypes.FIELD("event_id", DataTypes.STRING()),
        DataTypes.FIELD("customer_id", DataTypes.BIGINT()),
        DataTypes.FIELD("session_id", DataTypes.STRING()),
        DataTypes.FIELD("event_type", DataTypes.STRING()),
        DataTypes.FIELD("product_id", DataTypes.BIGINT()),
        DataTypes.FIELD(
            "event_timestamp",
            DataTypes.STRING(),
        ),
        DataTypes.FIELD("device", DataTypes.STRING()),
        DataTypes.FIELD("browser", DataTypes.STRING()),
        DataTypes.FIELD("ip_address", DataTypes.STRING()),
    ]),

    "support_tickets": DataTypes.ROW([
        DataTypes.FIELD("ticket_id", DataTypes.BIGINT()),
        DataTypes.FIELD("customer_id", DataTypes.BIGINT()),
        DataTypes.FIELD("created_at", DataTypes.STRING()),
        DataTypes.FIELD("category", DataTypes.STRING()),
        DataTypes.FIELD("priority", DataTypes.STRING()),
        DataTypes.FIELD("message", DataTypes.STRING()),
        DataTypes.FIELD("status", DataTypes.STRING()),
        DataTypes.FIELD(
            "resolution_time_minutes",
            DataTypes.INT(),
        ),
    ]),

    "marketing_events": DataTypes.ROW([
        DataTypes.FIELD("marketing_event_id", DataTypes.BIGINT()),
        DataTypes.FIELD("campaign_id", DataTypes.BIGINT()),
        DataTypes.FIELD("customer_id", DataTypes.BIGINT()),
        DataTypes.FIELD("campaign", DataTypes.STRING()),
        DataTypes.FIELD("channel", DataTypes.STRING()),
        DataTypes.FIELD("impression", DataTypes.BOOLEAN()),
        DataTypes.FIELD("click", DataTypes.BOOLEAN()),
        DataTypes.FIELD("conversion", DataTypes.BOOLEAN()),
        DataTypes.FIELD("cost", DataTypes.DECIMAL(12, 2)),
        DataTypes.FIELD(
            "event_timestamp",
            DataTypes.STRING(),
        ),
    ]),
}


SCHEMA_FIELD_NAMES = {
    "customers": [
        "customer_id", "first_name", "last_name", "email", "phone",
        "country", "state", "city", "signup_date", "customer_segment",
        "updated_at", "created_at",
    ],
    "products": [
        "product_id", "product_name", "category", "subcategory", "brand",
        "price", "cost", "supplier_id", "inventory_quantity", "created_at",
        "updated_at",
    ],
    "orders": [
        "order_id", "customer_id", "order_date", "status", "payment_method",
        "shipping_country", "shipping_state", "total_amount", "discount",
        "tax", "created_at", "updated_at",
    ],
    "order_items": [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price",
        "discount",
    ],
    "payments": [
        "payment_id", "order_id", "customer_id", "amount", "payment_method",
        "payment_status", "transaction_timestamp",
    ],
    "inventory": [
        "inventory_id", "product_id", "warehouse_id", "quantity",
        "reserved_quantity", "updated_at",
    ],
    "website_events": [
        "event_id", "customer_id", "session_id", "event_type", "product_id",
        "event_timestamp", "device", "browser", "ip_address",
    ],
    "support_tickets": [
        "ticket_id", "customer_id", "created_at", "category", "priority",
        "message", "status", "resolution_time_minutes",
    ],
    "marketing_events": [
        "marketing_event_id", "campaign_id", "customer_id", "campaign",
        "channel", "impression", "click", "conversion", "cost",
        "event_timestamp",
    ],
}

TIMESTAMP_FIELDS = {
    "signup_date", "updated_at", "created_at", "order_date",
    "transaction_timestamp", "event_timestamp",
}

STRING_FIELDS = {
    "first_name", "last_name", "email", "phone", "country", "state",
    "city", "signup_date", "customer_segment", "updated_at", "created_at",
    "product_name", "category", "subcategory", "brand", "order_date",
    "status", "payment_method", "shipping_country", "shipping_state",
    "transaction_timestamp", "event_id", "session_id", "event_type",
    "event_timestamp", "device", "browser", "ip_address", "category",
    "priority", "message", "campaign", "channel",
}


PYFLINK_ROW_TYPES = {
    "customers": Types.ROW_NAMED(
        ["customer_id", "first_name", "last_name", "email", "phone",
         "country", "state", "city", "signup_date", "customer_segment",
         "updated_at", "created_at"],
        [
            Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(),
        ],
    ),
    "products": Types.ROW_NAMED(
        ["product_id", "product_name", "category", "subcategory", "brand",
         "price", "cost", "supplier_id", "inventory_quantity", "created_at",
         "updated_at"],
        [
            Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.BIG_DEC(), Types.BIG_DEC(), Types.LONG(),
            Types.INT(), Types.STRING(), Types.STRING(),
        ],
    ),
    "orders": Types.ROW_NAMED(
        ["order_id", "customer_id", "order_date", "status", "payment_method",
         "shipping_country", "shipping_state", "total_amount", "discount",
         "tax", "created_at", "updated_at"],
        [
            Types.LONG(), Types.LONG(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.STRING(), Types.STRING(), Types.BIG_DEC(),
            Types.BIG_DEC(), Types.BIG_DEC(), Types.STRING(),
            Types.STRING(),
        ],
    ),
    "order_items": Types.ROW_NAMED(
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price",
         "discount"],
        [Types.LONG(), Types.LONG(), Types.LONG(), Types.INT(),
         Types.BIG_DEC(), Types.BIG_DEC()],
    ),
    "payments": Types.ROW_NAMED(
        ["payment_id", "order_id", "customer_id", "amount", "payment_method",
         "payment_status", "transaction_timestamp"],
        [Types.LONG(), Types.LONG(), Types.LONG(), Types.BIG_DEC(),
         Types.STRING(), Types.STRING(), Types.STRING()],
    ),
    "inventory": Types.ROW_NAMED(
        ["inventory_id", "product_id", "warehouse_id", "quantity",
         "reserved_quantity", "updated_at"],
        [Types.LONG(), Types.LONG(), Types.LONG(), Types.INT(), Types.INT(),
         Types.STRING()],
    ),
    "website_events": Types.ROW_NAMED(
        ["event_id", "customer_id", "session_id", "event_type", "product_id",
         "event_timestamp", "device", "browser", "ip_address"],
        [Types.STRING(), Types.LONG(), Types.STRING(), Types.STRING(),
        Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(),
         Types.STRING()],
    ),
    "support_tickets": Types.ROW_NAMED(
        ["ticket_id", "customer_id", "created_at", "category", "priority",
         "message", "status", "resolution_time_minutes"],
        [Types.LONG(), Types.LONG(), Types.STRING(), Types.STRING(),
         Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()],
    ),
    "marketing_events": Types.ROW_NAMED(
        ["marketing_event_id", "campaign_id", "customer_id", "campaign",
         "channel", "impression", "click", "conversion", "cost",
         "event_timestamp"],
        [Types.LONG(), Types.LONG(), Types.LONG(), Types.STRING(),
         Types.STRING(), Types.BOOLEAN(), Types.BOOLEAN(), Types.BOOLEAN(),
         Types.BIG_DEC(), Types.STRING()],
    ),
}


# ============================================================
# Helper: convert Debezium payload to typed Row
# ============================================================

def convert_value(value, field_name):
    if value is None:
        return None

    try:
        if field_name in STRING_FIELDS:
            return str(value)

        return value

    except Exception:
        return value


def create_table_row(record, table_name):
    """
    Extract the 'after' record for:
        r / c / u

    Extract the 'before' record for:
        d

    Then append CDC metadata.
    """

    op = record.op

    if op == "d":
        payload = record.before
    else:
        payload = record.after

    if payload is None:
        return None

    values = []

    for field_name in SCHEMA_FIELD_NAMES[table_name]:
        value = payload.get(field_name)

        values.append(
            convert_value(
                value,
                field_name,
            )
        )

    return Row(*values)


# ============================================================
# Build Kafka source
# ============================================================

def create_kafka_source(topic):

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(f"retailpulse-bronze-{topic}")
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )


# ============================================================
# FileSink
# ============================================================

def create_file_sink(table_name):

    schema = SCHEMAS[table_name]

    writer = ParquetBulkWriters.for_row_type(
        schema
    )

    sink = (
        FileSink.for_bulk_format(
            f"{MINIO_BASE}/{table_name}",
            writer,
        )
        .with_rolling_policy(
            OnCheckpointRollingPolicy(
                RollingPolicy.default_rolling_policy(
                    part_size=128 * 1024 * 1024,
                    rollover_interval=15 * 60 * 1000,
                )
            )
        )
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix(f"{table_name}-part")
            .with_part_suffix(".parquet")
            .build()
        )
        .build()
    )

    return sink


# ============================================================
# Main
# ============================================================

def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    # --------------------------------------------------------
    # Checkpointing
    # --------------------------------------------------------

    env.enable_checkpointing(10_000)

    checkpoint_config = env.get_checkpoint_config()

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
    # Build all nine CDC pipelines
    # --------------------------------------------------------

    for table_name, topic in KAFKA_TOPICS.items():

        print(
            f"Creating Bronze pipeline: "
            f"{table_name} -> {topic}"
        )

        source = create_kafka_source(topic)

        raw_stream = env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"{table_name}_kafka_source",
        )

        parsed_stream = (
            raw_stream
            .map(
                parse_debezium,
                output_type=Types.ROW_NAMED(
                    [
                        "op",
                        "ts_ms",
                        "before",
                        "after",
                    ],
                    [
                        Types.STRING(),
                        Types.LONG(),
                        Types.PICKLED_BYTE_ARRAY(),
                        Types.PICKLED_BYTE_ARRAY(),
                    ],
                ),
            )
            .filter(
                lambda x: x is not None
            )
        )

        # ----------------------------------------------------
        # Keep only valid CDC events
        # ----------------------------------------------------

        typed_stream = (
            parsed_stream
            .map(
                lambda record, table=table_name:
                    create_table_row(
                        record,
                        table,
                    ),
                output_type=PYFLINK_ROW_TYPES[table_name],
            )
            .filter(
                lambda x: x is not None
            )
        )

        # ----------------------------------------------------
        # FileSink
        # ----------------------------------------------------

        sink = create_file_sink(
            table_name
        )

        typed_stream.sink_to(
            sink
        ).name(
            f"{table_name}_bronze_filesink"
        )

    # --------------------------------------------------------
    # Execute
    # --------------------------------------------------------

    env.execute(
        "RetailPulse - Multi Table Bronze CDC"
    )


if __name__ == "__main__":
    main()