"""
RetailPulse AI - Production Stateful Streaming Gold
Customer 360

Architecture:

Kafka CDC
    |
    +-- customers
    +-- orders
    +-- payments
    +-- website_events
    +-- support_tickets
    +-- marketing_events
    |
    v
PyFlink
    |
    v
Union Customer Events
    |
    v
key_by(customer_id)
    |
    v
Flink Managed Keyed State
    |
    +-- customer profile
    +-- orders
    +-- payments
    +-- website events
    +-- support tickets
    +-- marketing events
    |
    v
Customer 360
    |
    v
MinIO / Parquet


IMPORTANT:

This version replaces normal Python dictionaries with
Flink-managed keyed state.

It also handles:

    r = snapshot/read
    c = create
    u = update
    d = delete

Updates and deletes are handled using entity-level state,
which prevents simple CDC updates from incorrectly
double-counting customer metrics.
"""

import json
from datetime import datetime, timezone

from pyflink.common import Row, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

from pyflink.datastream import (
    StreamExecutionEnvironment,
    KeyedProcessFunction,
)

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

from pyflink.table import StreamTableEnvironment

from pyflink.datastream.state import (
    ValueStateDescriptor,
    MapStateDescriptor,
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

GOLD_BASE = (
    "s3://retailpulse/streaming/gold"
)

GROUP_ID = (
    "retailpulse-streaming-gold-customer360-stateful-v1"
)


# ============================================================
# KAFKA TOPICS
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
# CUSTOMER 360 GOLD SCHEMA
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
# HELPER
# ============================================================

def safe_int(value):

    if value is None:
        return None

    try:
        return int(value)

    except (TypeError, ValueError):

        return None


def safe_float(value):

    if value is None:
        return None

    try:
        return float(value)

    except (TypeError, ValueError):

        return None


# ============================================================
# GENERIC DEBEZIUM PARSER
# ============================================================

def parse_debezium(
    message,
):

    try:

        data = json.loads(
            message
        )

        payload = data.get(
            "payload",
            data,
        )

        op = payload.get(
            "op"
        )

        ts_ms = safe_int(
            payload.get(
                "ts_ms"
            )
        )

        before = payload.get(
            "before"
        )

        after = payload.get(
            "after"
        )

        if op == "d":

            record = before

        else:

            record = after

        if record is None:

            return None

        return {
            "op": op,
            "ts_ms": ts_ms,
            "record": record,
        }

    except Exception:

        return None


# ============================================================
# CUSTOMER EVENT PARSER
# ============================================================

def parse_customer(
    message,
):

    data = parse_debezium(
        message
    )

    if data is None:
        return None

    record = data["record"]

    customer_id = safe_int(
        record.get(
            "customer_id"
        )
    )

    if customer_id is None:
        return None

    return {
        "event_type": "customer",

        "customer_id": customer_id,

        "entity_id": str(
            customer_id
        ),

        "record": record,

        "op": data["op"],

        "ts_ms": data["ts_ms"],
    }


# ============================================================
# ORDER EVENT PARSER
# ============================================================

def parse_order(
    message,
):

    data = parse_debezium(
        message
    )

    if data is None:
        return None

    record = data["record"]

    customer_id = safe_int(
        record.get(
            "customer_id"
        )
    )

    order_id = safe_int(
        record.get(
            "order_id"
        )
    )

    if customer_id is None:
        return None

    if order_id is None:
        return None

    return {
        "event_type": "order",

        "customer_id": customer_id,

        "entity_id": str(
            order_id
        ),

        "record": record,

        "op": data["op"],

        "ts_ms": data["ts_ms"],
    }


# ============================================================
# PAYMENT EVENT PARSER
# ============================================================

def parse_payment(
    message,
):

    data = parse_debezium(
        message
    )

    if data is None:
        return None

    record = data["record"]

    customer_id = safe_int(
        record.get(
            "customer_id"
        )
    )

    payment_id = safe_int(
        record.get(
            "payment_id"
        )
    )

    if customer_id is None:
        return None

    if payment_id is None:
        return None

    return {
        "event_type": "payment",

        "customer_id": customer_id,

        "entity_id": str(
            payment_id
        ),

        "record": record,

        "op": data["op"],

        "ts_ms": data["ts_ms"],
    }


# ============================================================
# WEBSITE EVENT PARSER
# ============================================================

def parse_website_event(
    message,
):

    data = parse_debezium(
        message
    )

    if data is None:
        return None

    record = data["record"]

    customer_id = safe_int(
        record.get(
            "customer_id"
        )
    )

    event_id = record.get(
        "event_id"
    )

    if customer_id is None:
        return None

    if event_id is None:
        return None

    return {
        "event_type": "website_event",

        "customer_id": customer_id,

        "entity_id": str(
            event_id
        ),

        "record": record,

        "op": data["op"],

        "ts_ms": data["ts_ms"],
    }


# ============================================================
# SUPPORT EVENT PARSER
# ============================================================

def parse_support_ticket(
    message,
):

    data = parse_debezium(
        message
    )

    if data is None:
        return None

    record = data["record"]

    customer_id = safe_int(
        record.get(
            "customer_id"
        )
    )

    ticket_id = safe_int(
        record.get(
            "ticket_id"
        )
    )

    if customer_id is None:
        return None

    if ticket_id is None:
        return None

    return {
        "event_type": "support_ticket",

        "customer_id": customer_id,

        "entity_id": str(
            ticket_id
        ),

        "record": record,

        "op": data["op"],

        "ts_ms": data["ts_ms"],
    }


# ============================================================
# MARKETING EVENT PARSER
# ============================================================

def parse_marketing_event(
    message,
):

    data = parse_debezium(
        message
    )

    if data is None:
        return None

    record = data["record"]

    customer_id = safe_int(
        record.get(
            "customer_id"
        )
    )

    marketing_event_id = safe_int(
        record.get(
            "marketing_event_id"
        )
    )

    if customer_id is None:
        return None

    if marketing_event_id is None:
        return None

    return {
        "event_type": "marketing_event",

        "customer_id": customer_id,

        "entity_id": str(
            marketing_event_id
        ),

        "record": record,

        "op": data["op"],

        "ts_ms": data["ts_ms"],
    }


# ============================================================
# STATEFUL CUSTOMER 360 PROCESSOR
# ============================================================

class Customer360StatefulProcessor(
    KeyedProcessFunction
):

    """
    One keyed state instance exists per customer_id.

    State:

        customer_state
            Latest customer profile

        orders_state
            order_id -> latest order

        payments_state
            payment_id -> latest payment

        website_events_state
            event_id -> latest website event

        support_tickets_state
            ticket_id -> latest ticket

        marketing_events_state
            marketing_event_id -> latest event
    """

    def open(
        self,
        runtime_context,
    ):

        # ----------------------------------------------------
        # Customer profile
        # ----------------------------------------------------

        customer_descriptor = (
            ValueStateDescriptor(
                "customer_profile",
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        self.customer_state = (
            runtime_context.get_state(
                customer_descriptor
            )
        )

        # ----------------------------------------------------
        # Orders
        # ----------------------------------------------------

        orders_descriptor = (
            MapStateDescriptor(
                "customer_orders",
                Types.STRING(),
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        self.orders_state = (
            runtime_context.get_map_state(
                orders_descriptor
            )
        )

        # ----------------------------------------------------
        # Payments
        # ----------------------------------------------------

        payments_descriptor = (
            MapStateDescriptor(
                "customer_payments",
                Types.STRING(),
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        self.payments_state = (
            runtime_context.get_map_state(
                payments_descriptor
            )
        )

        # ----------------------------------------------------
        # Website events
        # ----------------------------------------------------

        website_descriptor = (
            MapStateDescriptor(
                "customer_website_events",
                Types.STRING(),
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        self.website_events_state = (
            runtime_context.get_map_state(
                website_descriptor
            )
        )

        # ----------------------------------------------------
        # Support tickets
        # ----------------------------------------------------

        support_descriptor = (
            MapStateDescriptor(
                "customer_support_tickets",
                Types.STRING(),
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        self.support_tickets_state = (
            runtime_context.get_map_state(
                support_descriptor
            )
        )

        # ----------------------------------------------------
        # Marketing events
        # ----------------------------------------------------

        marketing_descriptor = (
            MapStateDescriptor(
                "customer_marketing_events",
                Types.STRING(),
                Types.PICKLED_BYTE_ARRAY(),
            )
        )

        self.marketing_events_state = (
            runtime_context.get_map_state(
                marketing_descriptor
            )
        )

    # ========================================================
    # PROCESS EVENT
    # ========================================================

    def process_element(
        self,
        event,
        ctx,
    ):

        if event is None:
            return

        event_type = event.get(
            "event_type"
        )

        op = event.get(
            "op"
        )

        entity_id = event.get(
            "entity_id"
        )

        record = event.get(
            "record"
        )

        # ----------------------------------------------------
        # Customer
        # ----------------------------------------------------

        if event_type == "customer":

            if op == "d":

                self.customer_state.clear()

            else:

                self.customer_state.update(
                    record
                )

        # ----------------------------------------------------
        # Order
        # ----------------------------------------------------

        elif event_type == "order":

            if op == "d":

                self.orders_state.remove(
                    entity_id
                )

            else:

                self.orders_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # Payment
        # ----------------------------------------------------

        elif event_type == "payment":

            if op == "d":

                self.payments_state.remove(
                    entity_id
                )

            else:

                self.payments_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # Website event
        # ----------------------------------------------------

        elif event_type == "website_event":

            if op == "d":

                self.website_events_state.remove(
                    entity_id
                )

            else:

                self.website_events_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # Support ticket
        # ----------------------------------------------------

        elif event_type == "support_ticket":

            if op == "d":

                self.support_tickets_state.remove(
                    entity_id
                )

            else:

                self.support_tickets_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # Marketing event
        # ----------------------------------------------------

        elif event_type == "marketing_event":

            if op == "d":

                self.marketing_events_state.remove(
                    entity_id
                )

            else:

                self.marketing_events_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # Emit updated customer 360
        # ----------------------------------------------------

        self.emit_customer_360(
            event
        )

    # ========================================================
    # CUSTOMER 360 AGGREGATION
    # ========================================================

    def emit_customer_360(
        self,
        event,
    ):

        customer = (
            self.customer_state.value()
        )

        # We cannot produce a customer profile
        # until the customer record exists.

        if customer is None:

            return

        # ----------------------------------------------------
        # Orders
        # ----------------------------------------------------

        total_orders = 0

        total_spend = 0.0

        last_order_date = None

        for order in (
            self.orders_state.values()
        ):

            total_orders += 1

            amount = safe_float(
                order.get(
                    "total_amount"
                )
            )

            if amount is not None:

                total_spend += amount

            order_date = order.get(
                "order_date"
            )

            if order_date is not None:

                if (
                    last_order_date is None
                    or str(order_date)
                    > str(last_order_date)
                ):

                    last_order_date = (
                        order_date
                    )

        # ----------------------------------------------------
        # Payments
        # ----------------------------------------------------

        total_payments = 0.0

        last_payment_date = None

        for payment in (
            self.payments_state.values()
        ):

            amount = safe_float(
                payment.get(
                    "amount"
                )
            )

            if amount is not None:

                total_payments += amount

            timestamp = payment.get(
                "transaction_timestamp"
            )

            if timestamp is not None:

                if (
                    last_payment_date is None
                    or str(timestamp)
                    > str(last_payment_date)
                ):

                    last_payment_date = (
                        timestamp
                    )

        # ----------------------------------------------------
        # Website
        # ----------------------------------------------------

        website_event_count = (
            self.website_events_state
            .items()
        )

        website_count = 0

        last_website_event = None

        for event_id, website_event in (
            website_event_count
        ):

            website_count += 1

            timestamp = website_event.get(
                "event_timestamp"
            )

            if timestamp is not None:

                if (
                    last_website_event is None
                    or str(timestamp)
                    > str(last_website_event)
                ):

                    last_website_event = (
                        timestamp
                    )

        # ----------------------------------------------------
        # Support
        # ----------------------------------------------------

        support_ticket_count = 0

        for ticket in (
            self.support_tickets_state.values()
        ):

            support_ticket_count += 1

        # ----------------------------------------------------
        # Marketing
        # ----------------------------------------------------

        marketing_impressions = 0

        marketing_clicks = 0

        marketing_conversions = 0

        for marketing_event in (
            self.marketing_events_state.values()
        ):

            marketing_impressions += (
                safe_int(
                    marketing_event.get(
                        "impression"
                    )
                )
                or 0
            )

            marketing_clicks += (
                safe_int(
                    marketing_event.get(
                        "click"
                    )
                )
                or 0
            )

            marketing_conversions += (
                safe_int(
                    marketing_event.get(
                        "conversion"
                    )
                )
                or 0
            )

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

        # ----------------------------------------------------
        # Latest event metadata
        # ----------------------------------------------------

        op = event.get(
            "op"
        )

        ts_ms = event.get(
            "ts_ms"
        )

        # ----------------------------------------------------
        # Output
        # ----------------------------------------------------

        def string_value(value):
            return str(value) if value is not None else None

        output = Row(

            safe_int(
                customer.get(
                    "customer_id"
                )
            ),

            string_value(customer.get("first_name")),

            string_value(customer.get("last_name")),

            string_value(customer.get("email")),

            string_value(customer.get("country")),

            string_value(customer.get("state")),

            string_value(customer.get("city")),

            string_value(customer.get("customer_segment")),

            total_orders,

            total_spend,

            total_payments,

            website_count,

            support_ticket_count,

            marketing_impressions,

            marketing_clicks,

            marketing_conversions,

            string_value(last_order_date),

            string_value(last_payment_date),

            string_value(last_website_event),

            string_value(customer_status),

            string_value(op),

            ts_ms,

            datetime.now(
                timezone.utc
            ).isoformat(),
        )

        # ----------------------------------------------------
        # Emit
        # ----------------------------------------------------

        self.ctx_output.collect(
            output
        )


# ============================================================
# NOTE:
# PyFlink process functions don't expose a direct output
# collector through self.
#
# We use a wrapper processor below to correctly collect output.
# ============================================================


class Customer360ProcessFunction(
    KeyedProcessFunction
):

    def open(
        self,
        runtime_context,
    ):

        # ----------------------------------------------------
        # Customer
        # ----------------------------------------------------

        self.customer_state = (
            runtime_context.get_state(
                ValueStateDescriptor(
                    "customer_profile",
                    Types.PICKLED_BYTE_ARRAY(),
                )
            )
        )

        # ----------------------------------------------------
        # Orders
        # ----------------------------------------------------

        self.orders_state = (
            runtime_context.get_map_state(
                MapStateDescriptor(
                    "customer_orders",
                    Types.STRING(),
                    Types.PICKLED_BYTE_ARRAY(),
                )
            )
        )

        # ----------------------------------------------------
        # Payments
        # ----------------------------------------------------

        self.payments_state = (
            runtime_context.get_map_state(
                MapStateDescriptor(
                    "customer_payments",
                    Types.STRING(),
                    Types.PICKLED_BYTE_ARRAY(),
                )
            )
        )

        # ----------------------------------------------------
        # Website
        # ----------------------------------------------------

        self.website_state = (
            runtime_context.get_map_state(
                MapStateDescriptor(
                    "customer_website_events",
                    Types.STRING(),
                    Types.PICKLED_BYTE_ARRAY(),
                )
            )
        )

        # ----------------------------------------------------
        # Support
        # ----------------------------------------------------

        self.support_state = (
            runtime_context.get_map_state(
                MapStateDescriptor(
                    "customer_support_tickets",
                    Types.STRING(),
                    Types.PICKLED_BYTE_ARRAY(),
                )
            )
        )

        # ----------------------------------------------------
        # Marketing
        # ----------------------------------------------------

        self.marketing_state = (
            runtime_context.get_map_state(
                MapStateDescriptor(
                    "customer_marketing_events",
                    Types.STRING(),
                    Types.PICKLED_BYTE_ARRAY(),
                )
            )
        )

    # ========================================================
    # PROCESS
    # ========================================================

    def process_element(
        self,
        event,
        ctx,
    ):

        if event is None:
            return

        event_type = event.get(
            "event_type"
        )

        op = event.get(
            "op"
        )

        entity_id = event.get(
            "entity_id"
        )

        record = event.get(
            "record"
        )

        # ----------------------------------------------------
        # CUSTOMER
        # ----------------------------------------------------

        if event_type == "customer":

            if op == "d":

                self.customer_state.clear()

            else:

                self.customer_state.update(
                    record
                )

        # ----------------------------------------------------
        # ORDER
        # ----------------------------------------------------

        elif event_type == "order":

            if op == "d":

                self.orders_state.remove(
                    entity_id
                )

            else:

                self.orders_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # PAYMENT
        # ----------------------------------------------------

        elif event_type == "payment":

            if op == "d":

                self.payments_state.remove(
                    entity_id
                )

            else:

                self.payments_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # WEBSITE
        # ----------------------------------------------------

        elif event_type == "website_event":

            if op == "d":

                self.website_state.remove(
                    entity_id
                )

            else:

                self.website_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # SUPPORT
        # ----------------------------------------------------

        elif event_type == "support_ticket":

            if op == "d":

                self.support_state.remove(
                    entity_id
                )

            else:

                self.support_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # MARKETING
        # ----------------------------------------------------

        elif event_type == "marketing_event":

            if op == "d":

                self.marketing_state.remove(
                    entity_id
                )

            else:

                self.marketing_state.put(
                    entity_id,
                    record
                )

        # ----------------------------------------------------
        # Current customer profile
        # ----------------------------------------------------

        customer = self.customer_state.value()

        if customer is None and event_type == "customer" and op != "d":
            customer = record

        if customer is None:

            return

        # ----------------------------------------------------
        # Orders
        # ----------------------------------------------------

        total_orders = 0

        total_spend = 0.0

        last_order_date = None

        for order in (
            self.orders_state.values()
        ):

            total_orders += 1

            amount = safe_float(
                order.get(
                    "total_amount"
                )
            )

            if amount is not None:

                total_spend += amount

            order_date = order.get(
                "order_date"
            )

            if order_date is not None:

                if (
                    last_order_date is None
                    or str(order_date)
                    > str(last_order_date)
                ):

                    last_order_date = (
                        order_date
                    )

        # ----------------------------------------------------
        # Payments
        # ----------------------------------------------------

        total_payments = 0.0

        last_payment_date = None

        for payment in (
            self.payments_state.values()
        ):

            amount = safe_float(
                payment.get(
                    "amount"
                )
            )

            if amount is not None:

                total_payments += amount

            timestamp = payment.get(
                "transaction_timestamp"
            )

            if timestamp is not None:

                if (
                    last_payment_date is None
                    or str(timestamp)
                    > str(last_payment_date)
                ):

                    last_payment_date = (
                        timestamp
                    )

        # ----------------------------------------------------
        # Website events
        # ----------------------------------------------------

        website_event_count = 0

        last_website_event = None

        for website_event in (
            self.website_state.values()
        ):

            website_event_count += 1

            timestamp = website_event.get(
                "event_timestamp"
            )

            if timestamp is not None:

                if (
                    last_website_event is None
                    or str(timestamp)
                    > str(last_website_event)
                ):

                    last_website_event = (
                        timestamp
                    )

        # ----------------------------------------------------
        # Support tickets
        # ----------------------------------------------------

        support_ticket_count = 0

        for ticket in (
            self.support_state.values()
        ):

            support_ticket_count += 1

        # ----------------------------------------------------
        # Marketing
        # ----------------------------------------------------

        marketing_impressions = 0

        marketing_clicks = 0

        marketing_conversions = 0

        for marketing_event in (
            self.marketing_state.values()
        ):

            marketing_impressions += (
                safe_int(
                    marketing_event.get(
                        "impression"
                    )
                )
                or 0
            )

            marketing_clicks += (
                safe_int(
                    marketing_event.get(
                        "click"
                    )
                )
                or 0
            )

            marketing_conversions += (
                safe_int(
                    marketing_event.get(
                        "conversion"
                    )
                )
                or 0
            )

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

        # ----------------------------------------------------
        # Build output
        # ----------------------------------------------------

        def string_value(value):
            return str(value) if value is not None else None

        output = Row(

            safe_int(
                customer.get(
                    "customer_id"
                )
            ),

            string_value(customer.get("first_name")),

            string_value(customer.get("last_name")),

            string_value(customer.get("email")),

            string_value(customer.get("country")),

            string_value(customer.get("state")),

            string_value(customer.get("city")),

            string_value(customer.get("customer_segment")),

            total_orders,

            total_spend,

            total_payments,

            website_event_count,

            support_ticket_count,

            marketing_impressions,

            marketing_clicks,

            marketing_conversions,

            string_value(last_order_date),

            string_value(last_payment_date),

            string_value(last_website_event),

            string_value(customer_status),

            string_value(op),

            event.get(
                "ts_ms"
            ),

            datetime.now(
                timezone.utc
            ).isoformat(),
        )

        yield output


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
        .set_topics(
            topic
        )
        .set_group_id(
            group_id
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

    sink_name = "gold_sink_customer_360_stateful"

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
            'path' = '{GOLD_BASE}/customer_360_stateful',
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

    print("=" * 75)

    print(
        "RetailPulse AI"
    )

    print(
        "Production Stateful Streaming Gold"
    )

    print(
        "Dataset: customer_360_stateful"
    )

    print("=" * 75)

    # --------------------------------------------------------
    # Environment
    # --------------------------------------------------------

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    table_env = StreamTableEnvironment.create(env)
    statement_set = table_env.create_statement_set()

    # One subtask per source keeps this six-topic local job within the
    # TaskManager's development slot budget.
    env.set_parallelism(1)

    # Filesystem sinks commit their pending Parquet files only after a
    # successful checkpoint.  Without this, the job can consume records but
    # Gold output never becomes visible in MinIO.
    env.enable_checkpointing(10_000)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_timeout(120_000)
    checkpoint_config.set_min_pause_between_checkpoints(30_000)
    checkpoint_config.set_max_concurrent_checkpoints(1)

    checkpoint_config.set_checkpoint_storage(
    "s3://retailpulse/flink-checkpoints/customer360-stateful"
    )
            # CUSTOMER SOURCE
    # --------------------------------------------------------

    customer_source = (
        create_kafka_source(
            CUSTOMERS_TOPIC,
            GROUP_ID + "-customers",
        )
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
        .uid("customer360-customers-source")
    )

    # --------------------------------------------------------
    # ORDERS SOURCE
    # --------------------------------------------------------

    order_source = (
        create_kafka_source(
            ORDERS_TOPIC,
            GROUP_ID + "-orders",
        )
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
        .uid("customer360-orders-source")
    )

    # --------------------------------------------------------
    # PAYMENTS SOURCE
    # --------------------------------------------------------

    payment_source = (
        create_kafka_source(
            PAYMENTS_TOPIC,
            GROUP_ID + "-payments",
        )
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
        .uid("customer360-payments-source")
    )

    # --------------------------------------------------------
    # WEBSITE EVENTS
    # --------------------------------------------------------

    website_source = (
        create_kafka_source(
            WEBSITE_EVENTS_TOPIC,
            GROUP_ID + "-website",
        )
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
        .uid("customer360-website-source")
    )

    # --------------------------------------------------------
    # SUPPORT TICKETS
    # --------------------------------------------------------

    support_source = (
        create_kafka_source(
            SUPPORT_TICKETS_TOPIC,
            GROUP_ID + "-support",
        )
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
        .uid("customer360-support-source")
    )

    # --------------------------------------------------------
    # MARKETING EVENTS
    # --------------------------------------------------------

    marketing_source = (
        create_kafka_source(
            MARKETING_EVENTS_TOPIC,
            GROUP_ID + "-marketing",
        )
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
        .uid("customer360-marketing-source")
    )

    # ========================================================
    # UNION
    # ========================================================

    all_events = (
        customers
        .union(
            orders,
            payments,
            website_events,
            support_tickets,
            marketing_events,
        )
    )

    # ========================================================
    # KEY BY CUSTOMER
    # ========================================================

    keyed_events = (
        all_events.key_by(
            lambda x: x["customer_id"],
            key_type=Types.LONG(),
        )
    )

    # ========================================================
    # GOLD ROW TYPE
    # ========================================================

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

    # ========================================================
    # PROCESS WITH FLINK MANAGED STATE
    # ========================================================

    customer_360 = (
        keyed_events
        .process(
            Customer360ProcessFunction(),
            output_type=gold_type,
        )
    )

    # ========================================================
    # SINK
    # ========================================================

    view_name = "gold_customer_360_stateful_view"
    table_env.create_temporary_view(
        view_name,
        customer_360,
    )

    create_gold_sink(
        table_env,
        statement_set,
        view_name,
    )

    # ========================================================
    # EXECUTE
    # ========================================================

    print(
        "Submitting production-stateful Customer 360..."
    )

    statement_set.execute()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
