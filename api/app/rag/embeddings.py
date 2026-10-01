from __future__ import annotations

import httpx

from app.core.config import settings


async def embed_text(
    text: str,
) -> list[float]:

    async with httpx.AsyncClient(
        timeout=120
    ) as client:

        response = await client.post(
            (
                settings.ollama_url.rstrip("/")
                + "/api/embed"
            ),
            json={
                "model": settings.embedding_model,
                "input": text,
            },
        )

        response.raise_for_status()

        data = response.json()

    embeddings = data.get(
        "embeddings",
        []
    )

    if not embeddings:

        raise RuntimeError(
            "Ollama returned no embedding."
        )

    return embeddings[0]