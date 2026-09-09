import json

from kafka import KafkaConsumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS


# ============================================================
# Configuration
# ============================================================

BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS.split(",")

TOPIC = "retailpulse.ecommerce.website_events"

GROUP_ID = "retailpulse-test-consumer"


# ============================================================
# Consumer
# ============================================================

def create_consumer():

    return KafkaConsumer(

        TOPIC,

        bootstrap_servers=BOOTSTRAP_SERVERS,

        group_id=GROUP_ID,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        value_deserializer=lambda value:
            json.loads(value.decode("utf-8")),

        key_deserializer=lambda key:
            key.decode("utf-8")
            if key
            else None,
    )


# ============================================================
# Main
# ============================================================

def main():

    consumer = create_consumer()

    print("=" * 70)
    print("RetailPulse AI")
    print("Kafka Test Consumer")
    print("=" * 70)

    print()
    print(
        f"Listening to topic: {TOPIC}"
    )

    try:

        for message in consumer:

            print()
            print("-" * 70)

            print(
                f"Partition: {message.partition}"
            )

            print(
                f"Offset: {message.offset}"
            )

            print(
                f"Key: {message.key}"
            )

            print(
                "Value:"
            )

            print(
                json.dumps(
                    message.value,
                    indent=2,
                )
            )

    except KeyboardInterrupt:

        print()
        print("Consumer stopped.")

    finally:

        consumer.close()


if __name__ == "__main__":
    main()
