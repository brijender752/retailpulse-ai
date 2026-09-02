from datetime import datetime, timedelta
import random

from faker import Faker

fake = Faker("en_US")


SEGMENTS = [
    "standard",
    "standard",
    "standard",
    "premium",
    "premium",
    "vip",
]


def generate_customers(count: int) -> list[dict]:
    customers = []

    start_date = datetime.now() - timedelta(days=1095)

    for _ in range(count):
        signup_date = fake.date_time_between(
            start_date=start_date,
            end_date=datetime.now(),
        )

        customers.append(
            {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "country": "USA",
                "state": fake.state(),
                "city": fake.city(),
                "signup_date": signup_date,
                "customer_segment": random.choice(SEGMENTS),
            }
        )

    return customers