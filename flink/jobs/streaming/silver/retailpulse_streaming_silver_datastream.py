import json
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)
from pyflink.formats.parquet import ParquetBulkWriters


# ============================================================
# CONFIG
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

SILVER_BASE = "s3://retailpulse/streaming/silver"

TABLES = {
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
# BUSINESS SCHEMAS
# ============================================================

SCHEMAS = {

    "customers": [
        ("customer_id", Types.LONG()),
        ("first_name", Types.STRING()),
        ("last_name", Types.STRING()),
        ("email", Types.STRING()),
        ("phone", Types.STRING()),
        ("country", Types.STRING()),
        ("state", Types.STRING()),
        ("city", Types.STRING()),
        ("signup_date", Types.STRING()),
        ("customer_segment", Types.STRING()),
        ("updated_at", Types.STRING()),
        ("created_at", Types.STRING()),
    ],

    "products": [
        ("product_id", Types.LONG()),
        ("product_name", Types.STRING()),
        ("category", Types.STRING()),
        ("subcategory", Types.STRING()),
        ("brand", Types.STRING()),
        ("price", Types.DOUBLE()),
        ("cost", Types.DOUBLE()),
        ("supplier_id", Types.LONG()),
        ("inventory_quantity", Types.LONG()),
        ("created_at", Types.STRING()),
        ("updated_at", Types.STRING()),
    ],

    "orders": [
        ("order_id", Types.LONG()),
        ("customer_id", Types.LONG()),
        ("order_date", Types.STRING()),
        ("status", Types.STRING()),
        ("payment_method", Types.STRING()),
        ("shipping_country", Types.STRING()),
        ("shipping_state", Types.STRING()),
        ("total_amount", Types.DOUBLE()),
        ("discount", Types.DOUBLE()),
        ("tax", Types.DOUBLE()),
        ("created_at", Types.STRING()),
        ("updated_at", Types.STRING()),
    ],

    "order_items": [
        ("order_item_id", Types.LONG()),
        ("order_id", Types.LONG()),
        ("product_id", Types.LONG()),
        ("quantity", Types.LONG()),
        ("unit_price", Types.DOUBLE()),
        ("discount", Types.DOUBLE()),
    ],

    "payments": [
        ("payment_id", Types.LONG()),
        ("order_id", Types.LONG()),
        ("customer_id", Types.LONG()),
        ("amount", Types.DOUBLE()),
        ("payment_method", Types.STRING()),
        ("payment_status", Types.STRING()),
        ("transaction_timestamp", Types.STRING()),
    ],

    "inventory": [
        ("inventory_id", Types.LONG()),
        ("product_id", Types.LONG()),
        ("warehouse_id", Types.LONG()),
        ("quantity", Types.LONG()),
        ("reserved_quantity", Types.LONG()),
        ("updated_at", Types.STRING()),
    ],

    "website_events": [
        ("event_id", Types.STRING()),
        ("customer_id", Types.LONG()),
        ("session_id", Types.STRING()),
        ("event_type", Types.STRING()),
        ("product_id", Types.LONG()),
        ("event_timestamp", Types.STRING()),
        ("device", Types.STRING()),
        ("browser", Types.STRING()),
        ("ip_address", Types.STRING()),
    ],

    "support_tickets": [
        ("ticket_id", Types.LONG()),
        ("customer_id", Types.LONG()),
        ("created_at", Types.STRING()),
        ("category", Types.STRING()),
        ("priority", Types.STRING()),
        ("message", Types.STRING()),
        ("status", Types.STRING()),
        ("resolution_time_minutes", Types.LONG()),
    ],

    "marketing_events": [
        ("marketing_event_id", Types.LONG()),
        ("campaign_id", Types.LONG()),
        ("customer_id", Types.LONG()),
        ("campaign", Types.STRING()),
        ("channel", Types.STRING()),
        ("impression", Types.LONG()),
        ("click", Types.LONG()),
        ("conversion", Types.LONG()),
        ("cost", Types.DOUBLE()),
        ("event_timestamp", Types.STRING()),
    ],
}


# ============================================================
# PRIMARY KEYS
# ============================================================

PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "inventory": "inventory_id",
    "website_events": "event_id",
    "support_tickets": "ticket_id",
    "marketing_events": "marketing_event_id",
}


# ============================================================
# CDC PARSER
# ============================================================

class DebeziumParser(MapFunction):

    def __init__(self, table_name):
        self.table_name = table_name

    def map(self, message):

        try:

            data = json.loads(message)

            payload = data.get("payload", data)

            op = payload.get("op")

            ts_ms = payload.get("ts_ms")

            before = payload.get("before")
            after = payload.get("after")

            # ------------------------------------------------
            # DELETE
            # ------------------------------------------------

            if op == "d":
                record = before

            else:
                record = after

            if record is None:
                return None

            result = {}

            for field_name, _ in SCHEMAS[self.table_name]:

                result[field_name] = record.get(
                    field_name
                )

            # ------------------------------------------------
            # CDC metadata
            # ------------------------------------------------

            result["_op"] = op

            result["_ts_ms"] = ts_ms

            result["_source_table"] = self.table_name

            result["_processed_at"] = (
                datetime.now(timezone.utc).isoformat()
            )

            return result

        except Exception as exc:

            print(
                f"ERROR parsing {self.table_name}: {exc}"
            )

            return None


# ============================================================
# SILVER TRANSFORMATION
# ============================================================

class StreamingSilverTransform(MapFunction):

    def __init__(self, table_name):

        self.table_name = table_name
        self.primary_key = PRIMARY_KEYS[table_name]

    def map(self, record):

        if record is None:
            return None

        op = record.get("_op")

        # ----------------------------------------------------
        # Normalize CDC operation
        # ----------------------------------------------------

        if op is not None:
            op = str(op).upper()

        record["_op"] = op

        # ----------------------------------------------------
        # Normalize timestamp
        # ----------------------------------------------------

        ts_ms = record.get("_ts_ms")

        if ts_ms is not None:

            try:
                record["_ts_ms"] = int(ts_ms)

            except (TypeError, ValueError):

                record["_ts_ms"] = None

        # ----------------------------------------------------
        # Validate operation
        # ----------------------------------------------------

        if op not in ("R", "C", "U", "D"):

            print(
                f"WARNING: Invalid CDC operation "
                f"{op} for {self.table_name}"
            )

            return None

        # ----------------------------------------------------
        # DELETE
        # ----------------------------------------------------

        if op == "D":

            return record

        # ----------------------------------------------------
        # Silver record
        # ----------------------------------------------------

        result = {}

        for field_name, _ in SCHEMAS[self.table_name]:

            result[field_name] = record.get(
                field_name
            )

        # Keep CDC information needed downstream.
        result["_op"] = op
        result["_ts_ms"] = record.get("_ts_ms")
        result["_source_table"] = self.table_name
        result["_silver_processed_at"] = (
            datetime.now(timezone.utc).isoformat()
        )

        return result


# ============================================================
# KAFKA SOURCE
# ============================================================

def create_kafka_source(topic, table_name):

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(
            f"retailpulse-streaming-silver-{table_name}"
        )
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )


# ============================================================
# PARQUET SCHEMA
# ============================================================

def create_row_type(table_name):

    fields = []

    for name, field_type in SCHEMAS[table_name]:

        if field_type == Types.LONG():
            fields.append(
                (name, Types.LONG())
            )

        elif field_type == Types.DOUBLE():
            fields.append(
                (name, Types.DOUBLE())
            )

        else:
            fields.append(
                (name, Types.STRING())
            )

    fields.extend(
        [
            ("_op", Types.STRING()),
            ("_ts_ms", Types.LONG()),
            ("_source_table", Types.STRING()),
            (
                "_silver_processed_at",
                Types.STRING(),
            ),
        ]
    )

    names = [
        field[0]
        for field in fields
    ]

    types = [
        field[1]
        for field in fields
    ]

    return RowTypeInfo(
        types,
        names
    )


# ============================================================
# DICT → ROW
# ============================================================

class DictToRow(MapFunction):

    def __init__(self, table_name):

        self.table_name = table_name
        self.row_type = create_row_type(table_name)

    def map(self, record):

        if record is None:
            return None

        values = []

        for field_name, _ in SCHEMAS[self.table_name]:

            values.append(
                record.get(field_name)
            )

        values.append(
            record.get("_op")
        )

        values.append(
            record.get("_ts_ms")
        )

        values.append(
            record.get("_source_table")
        )

        values.append(
            record.get("_silver_processed_at")
        )

        from pyflink.common import Row

        return Row(*values)


# ============================================================
# FILE SINK
# ============================================================

def create_file_sink(table_name):

    row_type = create_row_type(table_name)

    output_path = (
        f"{SILVER_BASE}/{table_name}"
    )

    return (
        FileSink
        .for_bulk_format(
            output_path,
            ParquetBulkWriters.for_row_type(
                row_type
            ),
        )
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix(
                f"{table_name}-part"
            )
            .with_part_suffix(".parquet")
            .build()
        )
        .with_rolling_policy(
            RollingPolicy.default_rolling_policy(
                rollover_interval=60_000,
                inactivity_interval=30_000,
                max_part_size=128 * 1024 * 1024,
            )
        )
        .build()
    )


# ============================================================
# BUILD TABLE PIPELINE
# ============================================================

def build_table_pipeline(env, table_name, topic):

    print(
        f"Building streaming Silver: "
        f"{table_name}"
    )

    source = create_kafka_source(
        topic,
        table_name
    )

    stream = (
        env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"Kafka - {table_name}"
        )
    )

    parsed = (
        stream
        .map(
            DebeziumParser(table_name)
        )
        .filter(
            lambda x: x is not None
        )
    )

    transformed = (
        parsed
        .map(
            StreamingSilverTransform(
                table_name
            )
        )
        .filter(
            lambda x: x is not None
        )
    )

    rows = (
        transformed
        .map(
            DictToRow(table_name),
            output_type=create_row_type(
                table_name
            )
        )
    )

    sink = create_file_sink(
        table_name
    )

    rows.sink_to(
        sink
    ).name(
        f"Silver Sink - {table_name}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 80)
    print("RetailPulse AI")
    print("STREAMING SILVER PIPELINE")
    print("=" * 80)

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

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
    # Build all streaming Silver pipelines
    # --------------------------------------------------------

    for table_name, topic in TABLES.items():

        build_table_pipeline(
            env,
            table_name,
            topic
        )

    # --------------------------------------------------------
    # Execute
    # --------------------------------------------------------

    env.execute(
        "RetailPulse - Streaming Silver"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()