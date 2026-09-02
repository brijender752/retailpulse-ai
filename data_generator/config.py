import os

from dotenv import load_dotenv

load_dotenv()


DATABASE_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "retailpulse"),
    "user": os.getenv("POSTGRES_USER", "retailpulse"),
    "password": os.getenv("POSTGRES_PASSWORD", "retailpulse"),
}


DATA_VOLUME = {
    "customers": 10_000,
    "products": 1_000,
    "orders": 50_000,
    "events": 500_000,
    "support_tickets": 10_000,
    "marketing_events": 50_000,
}