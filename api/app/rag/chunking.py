from __future__ import annotations


def chunk_text(
    text: str,
    chunk_size: int = 900,
    overlap: int = 150,
) -> list[str]:

    text = text.strip()

    if not text:

        return []

    chunks = []

    start = 0

    while start < len(text):

        end = min(
            start + chunk_size,
            len(text),
        )

        chunk = text[
            start:end
        ].strip()

        if chunk:

            chunks.append(
                chunk
            )

        if end >= len(text):

            break

        start = (
            end - overlap
        )

    return chunks