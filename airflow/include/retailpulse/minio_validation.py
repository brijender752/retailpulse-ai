from retailpulse.config import MINIO_BUCKET
from retailpulse.health import get_minio_client


REQUIRED_PREFIXES = {
    "bronze": "streaming/bronze/",
    "silver_stream": "streaming/silver/",
    "gold_stream": "streaming/gold/",
    "customer360_recovery": (
        "streaming/gold/customer_360_recovery/"
    ),
    "customer360_checkpoints": (
        "flink-checkpoints/customer360-recovery/"
    ),
}


def count_objects(
    prefix: str,
    max_scan: int = 1000,
) -> int:

    client = get_minio_client()

    count = 0

    for _ in client.list_objects(
        MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    ):
        count += 1

        if count >= max_scan:
            break

    return count


def validate_prefix(
    name: str,
    prefix: str,
) -> dict:

    count = count_objects(
        prefix
    )

    if count < 1:
        raise RuntimeError(
            f"No objects found for "
            f"{name}: s3://{MINIO_BUCKET}/{prefix}"
        )

    return {
        "dataset": name,
        "prefix": prefix,
        "objects_found": count,
    }


def validate_streaming_outputs() -> list[dict]:
    results = []

    for name, prefix in (
        REQUIRED_PREFIXES.items()
    ):
        results.append(
            validate_prefix(
                name,
                prefix,
            )
        )

    return results
