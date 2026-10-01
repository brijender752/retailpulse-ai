from __future__ import annotations

from qdrant_client import (
    QdrantClient,
)

from app.core.config import settings


def get_qdrant_client():

    return QdrantClient(
        url=settings.qdrant_url
    )