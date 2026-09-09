import subprocess

from streaming_kafka.configs.topics import TOPICS


# ============================================================
# Kafka Configuration
# ============================================================

KAFKA_CONTAINER = "retailpulse-kafka"

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_PREFIX = "retailpulse.ecommerce."


# ============================================================
# Create Topic
# ============================================================

def create_topic(
    topic_name: str,
    partitions: int,
    replication_factor: int,
):
    """
    Create a Kafka topic inside the Apache Kafka container.
    """

    command = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-topics.sh",

        "--bootstrap-server",
        BOOTSTRAP_SERVER,

        "--create",

        "--if-not-exists",

        "--topic",
        topic_name,

        "--partitions",
        str(partitions),

        "--replication-factor",
        str(replication_factor),
    ]

    print(f"Creating topic: {topic_name}")

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:

        print(
            f"✓ Topic ready: {topic_name}"
        )

    else:

        print(
            f"✗ Failed to create topic: {topic_name}"
        )

        if result.stdout:
            print("STDOUT:")
            print(result.stdout)

        if result.stderr:
            print("STDERR:")
            print(result.stderr)


# ============================================================
# Main
# ============================================================

def main():

    print("=" * 70)
    print("RetailPulse AI")
    print("Kafka Topic Creation")
    print("=" * 70)

    print(
        f"\nKafka broker: {BOOTSTRAP_SERVER}"
    )

    print(
        f"Kafka container: {KAFKA_CONTAINER}"
    )

    print()

    for topic_name, config in TOPICS.items():

        create_topic(
            topic_name=f"{TOPIC_PREFIX}{topic_name}",
            partitions=config["partitions"],
            replication_factor=config["replication_factor"],
        )

    print()
    print("=" * 70)
    print("TOPIC CREATION COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()