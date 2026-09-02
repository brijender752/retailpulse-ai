import random
from datetime import datetime, timedelta


ORDER_STATUSES = [
    "completed",
    "completed",
    "completed",
    "completed",
    "shipped",
    "processing",
    "cancelled",
    "returned",
]


PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "paypal",
    "apple_pay",
    "google_pay",
]


def generate_orders(
    count: int,
    customer_ids: list[int],
) -> list[dict]:

    orders = []

    start_date = datetime.now() - timedelta(days=365)

    for _ in range(count):

        order_date = start_date + timedelta(
            seconds=random.randint(
                0,
                int(
                    (
                        datetime.now() - start_date
                    ).total_seconds()
                ),
            )
        )

        discount = round(
            random.choice(
                [0, 0, 0, 5, 10, 15, 20]
            ),
            2,
        )

        tax = round(
            random.uniform(2, 50),
            2,
        )

        orders.append(
            {
                "customer_id": random.choice(customer_ids),
                "order_date": order_date,
                "status": random.choice(ORDER_STATUSES),
                "payment_method": random.choice(
                    PAYMENT_METHODS
                ),
                "shipping_country": "USA",
                "shipping_state": "California",
                "total_amount": 0,
                "discount": discount,
                "tax": tax,
            }
        )

    return orders

def generate_order_items(
    orders: list[dict],
    products: list[dict],
) -> tuple[list[dict], list[dict]]:

    order_items = []

    enriched_orders = []

    for order_index, order in enumerate(orders):

        number_of_items = random.choices(
            [1, 2, 3, 4, 5],
            weights=[35, 30, 20, 10, 5],
            k=1,
        )[0]

        subtotal = 0

        selected_products = random.sample(
            products,
            min(number_of_items, len(products)),
        )

        for product in selected_products:

            quantity = random.randint(1, 4)

            unit_price = float(product["price"])

            item_discount = round(
                random.choice([0, 0, 0, 5, 10]),
                2,
            )

            subtotal += (
                quantity * unit_price
            ) - item_discount

            order_items.append(
                {
                    "order_index": order_index,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": item_discount,
                }
            )

        total = max(
            0,
            subtotal
            - order["discount"]
            + order["tax"],
        )

        order["total_amount"] = round(
            total,
            2,
        )

        enriched_orders.append(order)

    return enriched_orders, order_items