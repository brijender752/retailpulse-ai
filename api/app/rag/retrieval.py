from __future__ import annotations

from app.core.config import settings

from app.rag.embeddings import (
    embed_text,
)

from app.rag.vector_store import (
    get_qdrant_client,
)


# Initial cosine similarity cutoff; tune against representative questions.
MIN_RELEVANCE_SCORE = 0.3


async def retrieve_context(
    question: str,
    limit: int = 5,
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

        if (
            point.score is not None
            and point.score < MIN_RELEVANCE_SCORE
        ):
            continue

        payload = (
            point.payload
            or {}
        )

        contexts.append(
            {
                "text": payload.get(
                    "text",
                    "",
                ),
                "source": payload.get(
                    "source",
                ),
                "heading": payload.get(
                    "heading",
                ),
                "score": (
                    float(point.score)
                    if point.score is not None
                    else None
                ),
            }
        )

    return contexts
