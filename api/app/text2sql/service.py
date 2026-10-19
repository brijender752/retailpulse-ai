import asyncio
from decimal import Decimal
from datetime import date, datetime

from app.text2sql.generator import generate_sql
from app.text2sql.validator import validate_sql
from app.text2sql.executor import execute_readonly_sql


def serialize_value(value):
    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    return value


async def answer_analytics_question(question: str) -> dict:
    generated = await generate_sql(question)

    if generated.get("supported") is not True:
        return {
            "supported": False,
            "reason": generated.get(
                "reason",
                "Question not supported by the reporting schema",
            ),
            "sql": None,
            "columns": [],
            "rows": [],
            "row_count": 0,
        }

    sql = validate_sql(generated.get("sql", ""))

    rows = await asyncio.to_thread(
        execute_readonly_sql,
        sql,
    )

    clean_rows = [
        {
            key: serialize_value(value)
            for key, value in row.items()
        }
        for row in rows
    ]

    return {
        "supported": True,
        "reason": None,
        "sql": sql,
        "columns": (
            list(clean_rows[0].keys())
            if clean_rows
            else []
        ),
        "rows": clean_rows,
        "row_count": len(clean_rows),
    }