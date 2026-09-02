import random


PAYMENT_STATUSES = [
    "success",
    "success",
    "success",
    "success",
    "success",
    "failed",
    "pending",
]


def generate_payment(
    order: dict,
    order_id: int,
) -> dict:

    return {
        "order_id": order_id,
        "customer_id": order["customer_id"],
        "amount": order["total_amount"],
        "payment_method": order["payment_method"],
        "payment_status": random.choice(
            PAYMENT_STATUSES
        ),
        "transaction_timestamp": order["order_date"],
    }