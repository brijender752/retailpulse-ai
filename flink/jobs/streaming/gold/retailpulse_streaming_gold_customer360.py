"""
RetailPulse AI - Streaming Gold
Customer 360

Customers
   +
Orders
   +
Payments
   +
Website Events
   +
Support Tickets
   +
Marketing Events
   ↓
PyFlink
   ↓
Customer-level state
   ↓
customer_360
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


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

GOLD_BASE = "s3://retailpulse/streaming/gold"

GROUP_ID = "retailpulse-streaming-gold-customer360"


# ============================================================
# TOPICS
# ============================================================

CUSTOMERS_TOPIC = (
    "retailpulse.ecommerce.customers"
)

ORDERS_TOPIC = (
    "retailpulse.ecommerce.orders"
)

PAYMENTS_TOPIC = (
    "retailpulse.ecommerce.payments"
)

WEBSITE_EVENTS_TOPIC = (
    "retailpulse.ecommerce.website_events"
)

SUPPORT_TICKETS_TOPIC = (
    "retailpulse.ecommerce.support_tickets"
)

MARKETING_EVENTS_TOPIC = (
    "retailpulse.ecommerce.marketing_events"
)


# ============================================================
# CUSTOMER 360 SCHEMA
# ============================================================

GOLD_FIELDS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country",
    "state",
    "city",
    "customer_segment",

    "total_orders",
    "total_spend",
    "total_payments",

    "website_event_count",
    "support_ticket_count",

    "marketing_impressions",
    "marketing_clicks",
    "marketing_conversions",

    "last_order_date",
    "last_payment_date",
    "last_website_event",

    "customer_status",

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
# CUSTOMER PARSER
# ============================================================

def parse_customer(message):

    data = parse_debezium(message)

    if data is None:
        return None

    record = data["record"]

    try:

        customer_id = (
            int(record["customer_id"])
            if record.get("customer_id") is not None
            else None
        )

        if customer_id is None:
            return None

        return {
            "type": "customer",
            "customer_id": customer_id,

            "first_name": record.get(
                "first_name"
            ),

            "last_name": record.get(
                "last_name"
            ),

            "email": record.get(
                "email"
            ),

            "country": record.get(
                "country"
            ),

            "state": record.get(
                "state"
            ),

            "city": record.get(
                "city"
            ),

            "customer_segment": record.get(
                "customer_segment"
            ),

            "op": data["op"],
            "ts_ms": data["ts_ms"],
        }

    except Exception:

        return None


# ============================================================
# ORDER PARSER
# ============================================================

def parse_order(message):

    data = parse_debezium(message)

    if data is None:
        return None

    record = data["record"]

    try:

        customer_id = (
            int(record["customer_id"])
            if record.get("customer_id") is not None
            else None
        )

        if customer_id is None:
            return None

        amount = (
            float(record["total_amount"])
            if record.get("total_amount") is not None
            else 0.0
        )

        return {
            "type": "order",

            "customer_id": customer_id,

            "order_id": (
                int(record["order_id"])
                if record.get("order_id") is not None
                else None
            ),

            "amount": amount,

            "order_date": record.get(
                "order_date"
            ),

            "op": data["op"],

            "ts_ms": data["ts_ms"],
        }

    except Exception:

        return None


# ============================================================
# PAYMENT PARSER
# ============================================================

def parse_payment(message):

    data = parse_debezium(message)

    if data is None:
        return None

    record = data["record"]

    try:

        customer_id = (
            int(record["customer_id"])
            if record.get("customer_id") is not None
            else None
        )

        if customer_id is None:
            return None

        amount = (
            float(record["amount"])
            if record.get("amount") is not None
            else 0.0
        )

        return {
            "type": "payment",

            "customer_id": customer_id,

            "payment_id": (
                int(record["payment_id"])
                if record.get("payment_id") is not None
                else None
            ),

            "amount": amount,

            "transaction_timestamp": record.get(
                "transaction_timestamp"
            ),

            "op": data["op"],

            "ts_ms": data["ts_ms"],
        }

    except Exception:

        return None


# ============================================================
# WEBSITE EVENT PARSER
# ============================================================

def parse_website_event(message):

    data = parse_debezium(message)

    if data is None:
        return None

    record = data["record"]

    try:

        customer_id = (
            int(record["customer_id"])
            if record.get("customer_id") is not None
            else None
        )

        if customer_id is None:
            return None

        return {
            "type": "website_event",

            "customer_id": customer_id,

            "event_id": record.get(
                "event_id"
            ),

            "event_timestamp": record.get(
                "event_timestamp"
            ),

            "op": data["op"],

            "ts_ms": data["ts_ms"],
        }

    except Exception:

        return None


# ============================================================
# SUPPORT TICKET PARSER
# ============================================================

def parse_support_ticket(message):

    data = parse_debezium(message)

    if data is None:
        return None

    record = data["record"]

    try:

        customer_id = (
            int(record["customer_id"])
            if record.get("customer_id") is not None
            else None
        )

        if customer_id is None:
            return None

        return {
            "type": "support_ticket",

            "customer_id": customer_id,

            "ticket_id": (
                int(record["ticket_id"])
                if record.get("ticket_id") is not None
                else None
            ),

            "created_at": record.get(
                "created_at"
            ),

            "op": data["op"],

            "ts_ms": data["ts_ms"],
        }

    except Exception:

        return None


# ============================================================
# MARKETING EVENT PARSER
# ============================================================

def parse_marketing_event(message):

    data = parse_debezium(message)

    if data is None:
        return None

    record = data["record"]

    try:

        customer_id = (
            int(record["customer_id"])
            if record.get("customer_id") is not None
            else None
        )

        if customer_id is None:
            return None

        return {
            "type": "marketing_event",

            "customer_id": customer_id,

            "marketing_event_id": (
                int(record["marketing_event_id"])
                if record.get("marketing_event_id") is not None
                else None
            ),

            "impression": (
                int(record["impression"])
                if record.get("impression") is not None
                else 0
            ),

            "click": (
                int(record["click"])
                if record.get("click") is not None
                else 0
            ),

            "conversion": (
                int(record["conversion"])
                if record.get("conversion") is not None
                else 0
            ),

            "event_timestamp": record.get(
                "event_timestamp"
            ),

            "op": data["op"],

            "ts_ms": data["ts_ms"],
        }

    except Exception:

        return None


# ============================================================
# CUSTOMER STATE
# ============================================================

class Customer360Processor:

    def __init__(self):

        self.customers = {}

        self.metrics = {}

    # --------------------------------------------------------
    # Customer
    # --------------------------------------------------------

    def process_customer(self, event):

        if event is None:
            return None

        customer_id = event[
            "customer_id"
        ]

        if event["op"] == "d":

            self.customers.pop(
                customer_id,
                None,
            )

        else:

            self.customers[
                customer_id
            ] = event

        return self.build_customer(
            customer_id,
            event,
        )

    # --------------------------------------------------------
    # Order
    # --------------------------------------------------------

    def process_order(self, event):

        if event is None:
            return None

        customer_id = event[
            "customer_id"
        ]

        if customer_id not in self.metrics:

            self.create_metrics(
                customer_id
            )

        metric = self.metrics[
            customer_id
        ]

        amount = event.get(
            "amount",
            0.0,
        )

        if event["op"] == "d":

            metric["total_orders"] -= 1

            metric["total_spend"] -= amount

        else:

            metric["total_orders"] += 1

            metric["total_spend"] += amount

        order_date = event.get(
            "order_date"
        )

        if order_date is not None:

            metric[
                "last_order_date"
            ] = order_date

        return self.build_customer(
            customer_id,
            event,
        )

    # --------------------------------------------------------
    # Payment
    # --------------------------------------------------------

    def process_payment(self, event):

        if event is None:
            return None

        customer_id = event[
            "customer_id"
        ]

        if customer_id not in self.metrics:

            self.create_metrics(
                customer_id
            )

        metric = self.metrics[
            customer_id
        ]

        amount = event.get(
            "amount",
            0.0,
        )

        if event["op"] == "d":

            metric[
                "total_payments"
            ] -= amount

        else:

            metric[
                "total_payments"
            ] += amount

        timestamp = event.get(
            "transaction_timestamp"
        )

        if timestamp is not None:

            metric[
                "last_payment_date"
            ] = timestamp

        return self.build_customer(
            customer_id,
            event,
        )

    # --------------------------------------------------------
    # Website Event
    # --------------------------------------------------------

    def process_website_event(
        self,
        event,
    ):

        if event is None:
            return None

        customer_id = event[
            "customer_id"
        ]

        if customer_id not in self.metrics:

            self.create_metrics(
                customer_id
            )

        metric = self.metrics[
            customer_id
        ]

        if event["op"] == "d":

            metric[
                "website_event_count"
            ] -= 1

        else:

            metric[
                "website_event_count"
            ] += 1

        timestamp = event.get(
            "event_timestamp"
        )

        if timestamp is not None:

            metric[
                "last_website_event"
            ] = timestamp

        return self.build_customer(
            customer_id,
            event,
        )

    # --------------------------------------------------------
    # Support
    # --------------------------------------------------------

    def process_support_ticket(
        self,
        event,
    ):

        if event is None:
            return None

        customer_id = event[
            "customer_id"
        ]

        if customer_id not in self.metrics:

            self.create_metrics(
                customer_id
            )

        metric = self.metrics[
            customer_id
        ]

        if event["op"] == "d":

            metric[
                "support_ticket_count"
            ] -= 1

        else:

            metric[
                "support_ticket_count"
            ] += 1

        return self.build_customer(
            customer_id,
            event,
        )

    # --------------------------------------------------------
    # Marketing
    # --------------------------------------------------------

    def process_marketing_event(
        self,
        event,
    ):

        if event is None:
            return None

        customer_id = event[
            "customer_id"
        ]

        if customer_id not in self.metrics:

            self.create_metrics(
                customer_id
            )

        metric = self.metrics[
            customer_id
        ]

        multiplier = (
            -1
            if event["op"] == "d"
            else 1
        )

        metric[
            "marketing_impressions"
        ] += (
            event.get(
                "impression",
                0,
            )
            * multiplier
        )

        metric[
            "marketing_clicks"
        ] += (
            event.get(
                "click",
                0,
            )
            * multiplier
        )

        metric[
            "marketing_conversions"
        ] += (
            event.get(
                "conversion",
                0,
            )
            * multiplier
        )

        return self.build_customer(
            customer_id,
            event,
        )

    # --------------------------------------------------------
    # Initialize metrics
    # --------------------------------------------------------

    def create_metrics(
        self,
        customer_id,
    ):

        self.metrics[
            customer_id
        ] = {
            "total_orders": 0,
            "total_spend": 0.0,
            "total_payments": 0.0,

            "website_event_count": 0,

            "support_ticket_count": 0,

            "marketing_impressions": 0,

            "marketing_clicks": 0,

            "marketing_conversions": 0,

            "last_order_date": None,

            "last_payment_date": None,

            "last_website_event": None,
        }

    # --------------------------------------------------------
    # Build Customer 360
    # --------------------------------------------------------

    def build_customer(
        self,
        customer_id,
        event,
    ):

        customer = self.customers.get(
            customer_id
        )

        if customer is None:

            return None

        if customer_id not in self.metrics:

            self.create_metrics(
                customer_id
            )

        metric = self.metrics[
            customer_id
        ]

        total_orders = metric[
            "total_orders"
        ]

        total_spend = metric[
            "total_spend"
        ]

        # ----------------------------------------------------
        # Customer status
        # ----------------------------------------------------

        if total_orders >= 10:

            customer_status = "VIP"

        elif total_orders >= 5:

            customer_status = "ACTIVE"

        elif total_orders >= 1:

            customer_status = "CUSTOMER"

        else:

            customer_status = "NEW"

        def string_value(value):
            return str(value) if value is not None else None

        return Row(
            customer_id,

            string_value(customer.get("first_name")),

            string_value(customer.get("last_name")),

            string_value(customer.get("email")),

            string_value(customer.get("country")),

            string_value(customer.get("state")),

            string_value(customer.get("city")),

            string_value(customer.get("customer_segment")),

            total_orders,

            total_spend,

            metric[
                "total_payments"
            ],

            metric[
                "website_event_count"
            ],

            metric[
                "support_ticket_count"
            ],

            metric[
                "marketing_impressions"
            ],

            metric[
                "marketing_clicks"
            ],

            metric[
                "marketing_conversions"
            ],

            string_value(metric["last_order_date"]),

            string_value(metric["last_payment_date"]),

            string_value(metric["last_website_event"]),

            customer_status,

            string_value(event.get("op")),

            event.get(
                "ts_ms"
            ),

            datetime.now(
                timezone.utc
            ).isoformat(),
        )


# ============================================================
# KAFKA SOURCE
# ============================================================

def create_kafka_source(
    topic,
):

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP
        )
        .set_topics(topic)
        .set_group_id(
            GROUP_ID
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
# GOLD PARQUET TABLE SINK
# ============================================================

def create_gold_sink(table_env, statement_set, view_name):

    sink_name = "gold_sink_customer_360"

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE `{sink_name}` (
            `customer_id` BIGINT,
            `first_name` STRING,
            `last_name` STRING,
            `email` STRING,
            `country` STRING,
            `state` STRING,
            `city` STRING,
            `customer_segment` STRING,
            `total_orders` BIGINT,
            `total_spend` DOUBLE,
            `total_payments` DOUBLE,
            `website_event_count` BIGINT,
            `support_ticket_count` BIGINT,
            `marketing_impressions` BIGINT,
            `marketing_clicks` BIGINT,
            `marketing_conversions` BIGINT,
            `last_order_date` STRING,
            `last_payment_date` STRING,
            `last_website_event` STRING,
            `customer_status` STRING,
            `_op` STRING,
            `_ts_ms` BIGINT,
            `_gold_processed_at` STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{GOLD_BASE}/customer_360',
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
        "Dataset: customer_360"
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
    # Customer source
    # --------------------------------------------------------

    customer_source = create_kafka_source(
        CUSTOMERS_TOPIC
    )

    customers = (
        env.from_source(
            customer_source,
            WatermarkStrategy.no_watermarks(),
            "Customers CDC",
        )
        .map(
            parse_customer,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Orders
    # --------------------------------------------------------

    order_source = create_kafka_source(
        ORDERS_TOPIC
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
    # Payments
    # --------------------------------------------------------

    payment_source = create_kafka_source(
        PAYMENTS_TOPIC
    )

    payments = (
        env.from_source(
            payment_source,
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
    # Website events
    # --------------------------------------------------------

    website_source = create_kafka_source(
        WEBSITE_EVENTS_TOPIC
    )

    website_events = (
        env.from_source(
            website_source,
            WatermarkStrategy.no_watermarks(),
            "Website Events CDC",
        )
        .map(
            parse_website_event,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Support tickets
    # --------------------------------------------------------

    support_source = create_kafka_source(
        SUPPORT_TICKETS_TOPIC
    )

    support_tickets = (
        env.from_source(
            support_source,
            WatermarkStrategy.no_watermarks(),
            "Support Tickets CDC",
        )
        .map(
            parse_support_ticket,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Marketing
    # --------------------------------------------------------

    marketing_source = create_kafka_source(
        MARKETING_EVENTS_TOPIC
    )

    marketing_events = (
        env.from_source(
            marketing_source,
            WatermarkStrategy.no_watermarks(),
            "Marketing Events CDC",
        )
        .map(
            parse_marketing_event,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # IMPORTANT
    # --------------------------------------------------------
    #
    # This first implementation validates the complete
    # Customer 360 data flow.
    #
    # Each stream contributes customer-level metrics.
    #
    # The next production-hardening phase will replace the
    # Python dictionaries with Flink-managed keyed state.
    #
    # --------------------------------------------------------

    processor = Customer360Processor()

    customer_rows = (
        customers
        .map(
            processor.process_customer,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    order_rows = (
        orders
        .map(
            processor.process_order,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    payment_rows = (
        payments
        .map(
            processor.process_payment,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    website_rows = (
        website_events
        .map(
            processor.process_website_event,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    support_rows = (
        support_tickets
        .map(
            processor.process_support_ticket,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    marketing_rows = (
        marketing_events
        .map(
            processor.process_marketing_event,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Gold Row Type
    # --------------------------------------------------------

    gold_type = Types.ROW_NAMED(
        GOLD_FIELDS,
        [
            Types.LONG(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),

            Types.LONG(),
            Types.DOUBLE(),
            Types.DOUBLE(),

            Types.LONG(),
            Types.LONG(),

            Types.LONG(),
            Types.LONG(),
            Types.LONG(),

            Types.STRING(),
            Types.STRING(),
            Types.STRING(),

            Types.STRING(),

            Types.STRING(),
            Types.LONG(),
            Types.STRING(),
        ],
    )

    # --------------------------------------------------------
    # Convert all streams to same Gold type
    # --------------------------------------------------------

    customer_gold = customer_rows.map(
        lambda x: x,
        output_type=gold_type,
    )

    order_gold = order_rows.map(
        lambda x: x,
        output_type=gold_type,
    )

    payment_gold = payment_rows.map(
        lambda x: x,
        output_type=gold_type,
    )

    website_gold = website_rows.map(
        lambda x: x,
        output_type=gold_type,
    )

    support_gold = support_rows.map(
        lambda x: x,
        output_type=gold_type,
    )

    marketing_gold = marketing_rows.map(
        lambda x: x,
        output_type=gold_type,
    )

    # --------------------------------------------------------
    # Combine
    # --------------------------------------------------------

    combined = (
        customer_gold
        .union(order_gold)
        .union(payment_gold)
        .union(website_gold)
        .union(support_gold)
        .union(marketing_gold)
    )

    # --------------------------------------------------------
    # Sink
    # --------------------------------------------------------

    view_name = "gold_customer_360_view"
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
        "Submitting Customer 360 job..."
    )

    statement_set.execute()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()