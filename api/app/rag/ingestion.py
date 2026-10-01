from __future__ import annotations

import hashlib
from pathlib import Path

from qdrant_client.models import (
    Distance,
    PointStruct,
    VectorParams,
)

from app.core.config import settings

from app.rag.chunking import (
    chunk_text,
)

from app.rag.embeddings import (
    embed_text,
)

from app.rag.vector_store import (
    get_qdrant_client,
)


KNOWLEDGE_DIR = Path(
    "/knowledge"
)


def stable_id(
    source: str,
    index: int,
) -> int:

    value = (
        f"{source}:{index}"
    )

    digest = hashlib.sha256(
        value.encode()
    ).hexdigest()

    return int(
        digest[:15],
        16,
    )


async def ingest_knowledge():

    client = get_qdrant_client()

    documents = list(
        KNOWLEDGE_DIR.glob("*.md")
    )

    if not documents:

        raise RuntimeError(
            "No knowledge documents found."
        )

    # Determine vector size from the actual
    # embedding model instead of hardcoding it.
    sample_vector = await embed_text(
        "RetailPulse"
    )

    vector_size = len(
        sample_vector
    )

    existing = [
        collection.name
        for collection
        in client.get_collections().collections
    ]

    if (
        settings.qdrant_collection
        not in existing
    ):

        client.create_collection(
            collection_name=(
                settings.qdrant_collection
            ),
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE,
            ),
        )

    points = []

    for document in documents:

        text = document.read_text(
            encoding="utf-8"
        )

        chunks = chunk_text(
            text
        )

        for index, chunk in enumerate(
            chunks
        ):

            vector = await embed_text(
                chunk
            )

            points.append(
                PointStruct(
                    id=stable_id(
                        document.name,
                        index,
                    ),
                    vector=vector,
                    payload={
                        "source": document.name,
                        "chunk_index": index,
                        "text": chunk,
                    },
                )
            )

    client.upsert(
        collection_name=(
            settings.qdrant_collection
        ),
        points=points,
    )

    return {
        "documents": len(
            documents
        ),
        "chunks": len(
            points
        ),
        "collection": (
            settings.qdrant_collection
        ),
    }