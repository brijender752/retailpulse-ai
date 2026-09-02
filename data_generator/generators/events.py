import random
import uuid
from datetime import datetime, timedelta


EVENT_TYPES = [
    "PAGE_VIEW",
    "PRODUCT_VIEW",
    "SEARCH",
    "ADD_TO_CART",
    "REMOVE_FROM_CART",
    "CHECKOUT",
    "PURCHASE",
    "LOGIN",
    "LOGOUT",
]


DEVICES = [
    "desktop",
    "mobile",
    "tablet",
]


BROWSERS = [
    "Chrome",
    "Safari",
    "Firefox",
    "Edge",
]


def generate_events(
    count: int,
    customer_ids: list[int],
    product_ids: list[int],
) -> list[dict]:

    events = []

    start_date = datetime.now() - timedelta(
        days=30
    )

    for _ in range(count):

        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "customer_id": random.choice(
                    customer_ids
                ),
                "session_id": str(uuid.uuid4()),
                "event_type": random.choices(
                    EVENT_TYPES,
                    weights=[
                        35,
                        25,
                        8,
                        10,
                        3,
                        5,
                        4,
                        6,
                        4,
                    ],
                    k=1,
                )[0],
                "product_id": random.choice(
                    product_ids
                ),
                "event_timestamp": start_date
                + timedelta(
                    seconds=random.randint(
                        0,
                        30 * 24 * 60 * 60,
                    )
                ),
                "device": random.choice(
                    DEVICES
                ),
                "browser": random.choice(
                    BROWSERS
                ),
                "ip_address": (
                    f"{random.randint(1, 255)}."
                    f"{random.randint(0, 255)}."
                    f"{random.randint(0, 255)}."
                    f"{random.randint(1, 255)}"
                ),
            }
        )

    return events