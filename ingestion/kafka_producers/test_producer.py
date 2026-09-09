import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS


# ============================================================
# Configuration
# ============================================================

BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS.split(",")

TOPIC = "retailpulse.ecommerce.website_events"


# ============================================================
# Producer
# ============================================================

def create_producer():

    return KafkaProducer(

        bootstrap_servers=BOOTSTRAP_SERVERS,

        value_serializer=lambda value:
            json.dumps(value).encode("utf-8"),

        key_serializer=lambda key:
            key.encode("utf-8")
            if key
            else None,

        acks="all",

        retries=5,

        linger_ms=10,

        compression_type="gzip",
    )


# ============================================================
# Generate Event
# ============================================================

def generate_event():

    event = {

        "event_id": str(uuid.uuid4()),

        "customer_id": 1001,

        "session_id": str(uuid.uuid4()),

        "event_type": "PRODUCT_VIEW",

        "product_id": 101,

        "event_timestamp": (
            datetime.now(timezone.utc)
            .isoformat()
        ),

        "device": "desktop",

        "browser": "chrome",

        "ip_address": "127.0.0.1",

    }

    return {
        "payload": {
            "op": "c",
            "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            "before": None,
            "after": event,
        }
    }


# ============================================================
# Main
# ============================================================

def main():

    producer = create_producer()

    print("=" * 70)
    print("RetailPulse AI")
    print("Kafka Test Producer")
    print("=" * 70)

    try:

        for i in range(20):

            event = generate_event()

            producer.send(
                TOPIC,
                key=event["payload"]["after"]["event_id"],
                value=event,
            )

            print(
                f"Sent event {i + 1}/20: "
                f"{event['payload']['after']['event_id']}"
            )

            time.sleep(1)

        producer.flush()

        print()
        print("20 events sent successfully.")

    finally:

        producer.close()


if __name__ == "__main__":
    main()
