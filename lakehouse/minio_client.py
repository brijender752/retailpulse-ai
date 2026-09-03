from minio import Minio

from config.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)


BUCKET_NAME = "retailpulse"


def get_minio_client():

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def create_bucket():

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):

        client.make_bucket(BUCKET_NAME)

        print(
            f"Created bucket: {BUCKET_NAME}"
        )

    else:

        print(
            f"Bucket already exists: {BUCKET_NAME}"
        )


if __name__ == "__main__":

    create_bucket()