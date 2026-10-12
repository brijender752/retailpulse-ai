from __future__ import annotations

import re


def split_markdown_sections(
    text: str,
) -> list[dict]:

    lines = text.splitlines()

    sections = []

    current_heading = "Introduction"
    current_lines = []

    for line in lines:

        heading = re.match(
            r"^(#{1,6})\s+(.+)$",
            line.strip(),
        )

        if heading:

            if current_lines:

                body = "\n".join(
                    current_lines
                ).strip()

                if body:

                    sections.append(
                        {
                            "heading": current_heading,
                            "text": body,
                        }
                    )

            current_heading = (
                heading.group(2).strip()
            )

            current_lines = []

        else:

            current_lines.append(
                line
            )

    if current_lines:

        body = "\n".join(
            current_lines
        ).strip()

        if body:

            sections.append(
                {
                    "heading": current_heading,
                    "text": body,
                }
            )

    return sections


def chunk_section(
    heading: str,
    text: str,
    chunk_size: int = 1000,
    overlap: int = 150,
) -> list[dict]:

    full_text = (
        f"{heading}\n\n{text}"
    ).strip()

    if len(full_text) <= chunk_size:

        return [
            {
                "heading": heading,
                "text": full_text,
            }
        ]

    chunks = []

    start = 0

    while start < len(full_text):

        end = min(
            start + chunk_size,
            len(full_text),
        )

        chunk = (
            full_text[start:end]
            .strip()
        )

        if chunk:

            chunks.append(
                {
                    "heading": heading,
                    "text": chunk,
                }
            )

        if end >= len(full_text):
            break

        start = end - overlap

    return chunks


def chunk_markdown(
    text: str,
) -> list[dict]:

    sections = (
        split_markdown_sections(
            text
        )
    )

    chunks = []

    for section in sections:

        section_chunks = (
            chunk_section(
                heading=section["heading"],
                text=section["text"],
            )
        )

        chunks.extend(
            section_chunks
        )

    return chunks