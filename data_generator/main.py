import time

from config.settings import (
    POSTGRES_HOST,
)

from data_generator.config import DATA_VOLUME
from data_generator.database import (
    bulk_insert,
    get_connection,
)

from data_generator.generators.customers import (
    generate_customers,
)

from data_generator.generators.products import (
    generate_products,
)

from data_generator.generators.orders import (
    generate_orders,
    generate_order_items,
)

from data_generator.generators.payments import (
    generate_payment,
)

from data_generator.generators.inventory import (
    generate_inventory,
)

from data_generator.generators.events import (
    generate_events,
)

from data_generator.generators.support import (
    generate_support_tickets,
)

from data_generator.generators.marketing import (
    generate_marketing_events,
)


def load_customers(connection, customers):

    rows = [
        (
            customer["first_name"],
            customer["last_name"],
            customer["email"],
            customer["phone"],
            customer["country"],
            customer["state"],
            customer["city"],
            customer["signup_date"],
            customer["customer_segment"],
        )
        for customer in customers
    ]

    bulk_insert(
        connection,
        "ecommerce.customers",
        [
            "first_name",
            "last_name",
            "email",
            "phone",
            "country",
            "state",
            "city",
            "signup_date",
            "customer_segment",
        ],
        rows,
    )


def load_products(connection, products):

    rows = [
        (
            product["product_name"],
            product["category"],
            product["subcategory"],
            product["brand"],
            product["price"],
            product["cost"],
            product["supplier_id"],
            product["inventory_quantity"],
        )
        for product in products
    ]

    bulk_insert(
        connection,
        "ecommerce.products",
        [
            "product_name",
            "category",
            "subcategory",
            "brand",
            "price",
            "cost",
            "supplier_id",
            "inventory_quantity",
        ],
        rows,
    )


def load_orders(connection, orders):

    rows = [
        (
            order["customer_id"],
            order["order_date"],
            order["status"],
            order["payment_method"],
            order["shipping_country"],
            order["shipping_state"],
            order["total_amount"],
            order["discount"],
            order["tax"],
        )
        for order in orders
    ]

    bulk_insert(
        connection,
        "ecommerce.orders",
        [
            "customer_id",
            "order_date",
            "status",
            "payment_method",
            "shipping_country",
            "shipping_state",
            "total_amount",
            "discount",
            "tax",
        ],
        rows,
    )


def load_order_items(
    connection,
    order_items,
    order_ids,
):

    rows = [
        (
            order_ids[item["order_index"]],
            item["product_id"],
            item["quantity"],
            item["unit_price"],
            item["discount"],
        )
        for item in order_items
    ]

    bulk_insert(
        connection,
        "ecommerce.order_items",
        [
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount",
        ],
        rows,
    )


def load_payments(
    connection,
    payments,
):

    rows = [
        (
            payment["order_id"],
            payment["customer_id"],
            payment["amount"],
            payment["payment_method"],
            payment["payment_status"],
            payment["transaction_timestamp"],
        )
        for payment in payments
    ]

    bulk_insert(
        connection,
        "ecommerce.payments",
        [
            "order_id",
            "customer_id",
            "amount",
            "payment_method",
            "payment_status",
            "transaction_timestamp",
        ],
        rows,
    )


def load_inventory(
    connection,
    inventory,
):

    rows = [
        (
            item["product_id"],
            item["warehouse_id"],
            item["quantity"],
            item["reserved_quantity"],
        )
        for item in inventory
    ]

    bulk_insert(
        connection,
        "ecommerce.inventory",
        [
            "product_id",
            "warehouse_id",
            "quantity",
            "reserved_quantity",
        ],
        rows,
    )


def load_events(
    connection,
    events,
):

    rows = [
        (
            event["event_id"],
            event["customer_id"],
            event["session_id"],
            event["event_type"],
            event["product_id"],
            event["event_timestamp"],
            event["device"],
            event["browser"],
            event["ip_address"],
        )
        for event in events
    ]

    bulk_insert(
        connection,
        "ecommerce.website_events",
        [
            "event_id",
            "customer_id",
            "session_id",
            "event_type",
            "product_id",
            "event_timestamp",
            "device",
            "browser",
            "ip_address",
        ],
        rows,
    )

def load_support_tickets(
    connection,
    tickets,
):

    rows = [
        (
            ticket["customer_id"],
            ticket["created_at"],
            ticket["category"],
            ticket["priority"],
            ticket["message"],
            ticket["status"],
            ticket["resolution_time_minutes"],
        )
        for ticket in tickets
    ]

    bulk_insert(
        connection,
        "ecommerce.support_tickets",
        [
            "customer_id",
            "created_at",
            "category",
            "priority",
            "message",
            "status",
            "resolution_time_minutes",
        ],
        rows,
    )


def load_marketing_events(
    connection,
    events,
):

    rows = [
        (
            event["campaign_id"],
            event["customer_id"],
            event["campaign"],
            event["channel"],
            event["impression"],
            event["click"],
            event["conversion"],
            event["cost"],
            event["event_timestamp"],
        )
        for event in events
    ]

    bulk_insert(
        connection,
        "ecommerce.marketing_events",
        [
            "campaign_id",
            "customer_id",
            "campaign",
            "channel",
            "impression",
            "click",
            "conversion",
            "cost",
            "event_timestamp",
        ],
        rows,
    )


def get_database_ids(connection):

    with connection.cursor() as cursor:

        cursor.execute(
            """
            SELECT customer_id
            FROM ecommerce.customers
            ORDER BY customer_id
            """
        )

        customer_ids = [
            row[0]
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT product_id
            FROM ecommerce.products
            ORDER BY product_id
            """
        )

        product_ids = [
            row[0]
            for row in cursor.fetchall()
        ]

        return customer_ids, product_ids


def get_order_ids(connection):

    with connection.cursor() as cursor:

        cursor.execute(
            """
            SELECT order_id
            FROM ecommerce.orders
            ORDER BY order_id
            """

        )

        return [
            row[0]
            for row in cursor.fetchall()
        ]


def main():

    start = time.perf_counter()

    print("=" * 60)
    print("RetailPulse AI - Data Generator")
    print("=" * 60)

    print(f"PostgreSQL host: {POSTGRES_HOST}")
    print()

    with get_connection() as connection:

        # -------------------------------------------------
        # 1. Customers
        # -------------------------------------------------

        print("Generating customers...")

        customers = generate_customers(
            DATA_VOLUME["customers"]
        )

        load_customers(
            connection,
            customers,
        )

        print(
            f"✓ Loaded {len(customers):,} customers"
        )

        # -------------------------------------------------
        # 2. Products
        # -------------------------------------------------

        print("Generating products...")

        products = generate_products(
            DATA_VOLUME["products"]
        )

        load_products(
            connection,
            products,
        )

        # Retrieve real database IDs
        _, product_ids = get_database_ids(
            connection
        )

        for product, product_id in zip(
            products,
            product_ids,
        ):
            product["product_id"] = product_id

        print(
            f"✓ Loaded {len(products):,} products"
        )

        # -------------------------------------------------
        # 3. Orders
        # -------------------------------------------------

        customer_ids, _ = get_database_ids(
            connection
        )

        print("Generating orders...")

        orders = generate_orders(
            DATA_VOLUME["orders"],
            customer_ids,
        )

        orders, order_items = (
            generate_order_items(
                orders,
                products,
            )
        )

        load_orders(
            connection,
            orders,
        )

        order_ids = get_order_ids(
            connection
        )

        load_order_items(
            connection,
            order_items,
            order_ids,
        )

        print(
            f"✓ Loaded {len(orders):,} orders"
        )

        print(
            f"✓ Loaded {len(order_items):,} order items"
        )

        # -------------------------------------------------
        # 4. Payments
        # -------------------------------------------------

        print("Generating payments...")

        payments = []

        for order_id, order in zip(
            order_ids,
            orders,
        ):

            payments.append(
                generate_payment(
                    order,
                    order_id,
                )
            )

        load_payments(
            connection,
            payments,
        )

        print(
            f"✓ Loaded {len(payments):,} payments"
        )

        # -------------------------------------------------
        # 5. Inventory
        # -------------------------------------------------

        print("Generating inventory...")

        inventory = generate_inventory(
            products
        )

        load_inventory(
            connection,
            inventory,
        )

        print(
            f"✓ Loaded {len(inventory):,} inventory records"
        )

        # -------------------------------------------------
        # 6. Website Events
        # -------------------------------------------------

        print("Generating website events...")

        events = generate_events(
            DATA_VOLUME["events"],
            customer_ids,
            product_ids,
        )

        load_events(
            connection,
            events,
        )

        print(
            f"✓ Loaded {len(events):,} website events"
        )

        # -------------------------------------------------
        # 7. Support
        # -------------------------------------------------

        print("Generating support tickets...")

        tickets = generate_support_tickets(
            DATA_VOLUME["support_tickets"],
            customer_ids,
        )

        load_support_tickets(
            connection,
            tickets,
        )

        print(
            f"✓ Loaded {len(tickets):,} support tickets"
        )

        # -------------------------------------------------
        # 8. Marketing
        # -------------------------------------------------

        print("Generating marketing events...")

        marketing_events = (
            generate_marketing_events(
                DATA_VOLUME["marketing_events"],
                customer_ids,
            )
        )

        load_marketing_events(
            connection,
            marketing_events,
        )

        print(
            f"✓ Loaded "
            f"{len(marketing_events):,} "
            f"marketing events"
        )

    elapsed = time.perf_counter() - start

    print()
    print("=" * 60)
    print(
        f"Completed in {elapsed:.2f} seconds"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()