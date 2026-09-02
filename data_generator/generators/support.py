import random
from datetime import datetime, timedelta

from faker import Faker


fake = Faker("en_US")


CATEGORIES = [
    "delivery",
    "payment",
    "refund",
    "product",
    "account",
    "technical",
]


PRIORITIES = [
    "low",
    "medium",
    "high",
    "critical",
]


STATUSES = [
    "open",
    "in_progress",
    "resolved",
]


def generate_support_tickets(
    count: int,
    customer_ids: list[int],
) -> list[dict]:

    tickets = []

    start_date = datetime.now() - timedelta(
        days=180
    )

    for _ in range(count):

        category = random.choice(
            CATEGORIES
        )

        tickets.append(
            {
                "customer_id": random.choice(
                    customer_ids
                ),
                "created_at": start_date
                + timedelta(
                    seconds=random.randint(
                        0,
                        180 * 24 * 60 * 60,
                    )
                ),
                "category": category,
                "priority": random.choice(
                    PRIORITIES
                ),
                "message": generate_message(
                    category
                ),
                "status": random.choice(
                    STATUSES
                ),
                "resolution_time_minutes": random.randint(
                    5,
                    2880,
                ),
            }
        )

    return tickets


def generate_message(category: str) -> str:

    messages = {
        "delivery": [
            "My order has not arrived yet.",
            "The delivery is late.",
            "Where is my package?",
        ],
        "payment": [
            "My payment failed.",
            "I was charged twice.",
            "Payment is not going through.",
        ],
        "refund": [
            "I haven't received my refund.",
            "Please process my refund.",
        ],
        "product": [
            "The product arrived damaged.",
            "The product is not working.",
            "I received the wrong product.",
        ],
        "account": [
            "I cannot log into my account.",
            "I need to change my account information.",
        ],
        "technical": [
            "The website is not working.",
            "The checkout page is broken.",
        ],
    }

    return random.choice(
        messages[category]
    )