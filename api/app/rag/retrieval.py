from __future__ import annotations

from app.core.config import settings

from app.rag.embeddings import (
    embed_text,
)

from app.rag.vector_store import (
    get_qdrant_client,
)


async def retrieve_context(
    question: str,
    limit: int = 4,
) -> list[dict]:

    vector = await embed_text(
        question
    )

    client = get_qdrant_client()

    result = client.query_points(
        collection_name=(
            settings.qdrant_collection
        ),
        query=vector,
        limit=limit,
        with_payload=True,
    )

    contexts = []

    for point in result.points:

        payload = (
            point.payload
            or {}
        )

        contexts.append(
            {
                "text": payload.get(
                    "text",
                    ""
                ),
                "source": payload.get(
                    "source"
                ),
                "score": point.score,
            }
        )

    return contexts