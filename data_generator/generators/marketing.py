import random
from datetime import datetime, timedelta


CAMPAIGNS = [
    "Summer Sale",
    "Black Friday",
    "Holiday Campaign",
    "New Customer",
    "Retargeting",
    "Product Launch",
]


CHANNELS = [
    "google_ads",
    "facebook",
    "instagram",
    "email",
    "affiliate",
]


def generate_marketing_events(
    count: int,
    customer_ids: list[int],
) -> list[dict]:

    events = []

    start_date = datetime.now() - timedelta(
        days=90
    )

    for _ in range(count):

        impression = True

        click = random.random() < 0.15

        conversion = (
            click
            and random.random() < 0.08
        )

        events.append(
            {
                "campaign_id": random.randint(
                    1,
                    20,
                ),
                "customer_id": random.choice(
                    customer_ids
                ),
                "campaign": random.choice(
                    CAMPAIGNS
                ),
                "channel": random.choice(
                    CHANNELS
                ),
                "impression": impression,
                "click": click,
                "conversion": conversion,
                "cost": round(
                    random.uniform(
                        0.01,
                        5.0,
                    ),
                    2,
                ),
                "event_timestamp": start_date
                + timedelta(
                    seconds=random.randint(
                        0,
                        90 * 24 * 60 * 60,
                    )
                ),
            }
        )

    return events