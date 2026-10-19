from uuid import UUID, uuid4

from app.db.postgres import get_connection


def ensure_session(session_id: UUID | None) -> UUID:
    session_id = session_id or uuid4()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai.conversations (session_id)
                VALUES (%s)
                ON CONFLICT (session_id) DO NOTHING
                """,
                (str(session_id),),
            )

    return session_id


def load_history(
    session_id: UUID,
    limit: int = 12,
) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT role, content
                FROM (
                    SELECT message_id, role, content
                    FROM ai.messages
                    WHERE session_id = %s
                    ORDER BY message_id DESC
                    LIMIT %s
                ) recent
                ORDER BY message_id ASC
                """,
                (str(session_id), limit),
            )
            rows = cur.fetchall()

    return [
        {"role": role, "content": content}
        for role, content in rows
    ]


def save_exchange(
    session_id: UUID,
    question: str,
    answer: str,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ai.messages
                    (session_id, role, content)
                VALUES (%s, %s, %s)
                """,
                [
                    (str(session_id), "user", question),
                    (str(session_id), "assistant", answer),
                ],
            )
            cur.execute(
                """
                UPDATE ai.conversations
                SET updated_at = NOW()
                WHERE session_id = %s
                """,
                (str(session_id),),
            )