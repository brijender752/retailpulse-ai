import json
import httpx

from app.core.config import settings
from app.text2sql.schema import SCHEMA_DESCRIPTION


async def generate_sql(question: str) -> dict:
    system_prompt = f"""
You are a PostgreSQL analytics SQL generator.

{SCHEMA_DESCRIPTION}

Return only a JSON object:
{{
  "sql": "SELECT ...",
  "supported": true,
  "reason": ""
}}

If the question cannot be answered from the allowed
schema, return:
{{
  "sql": "",
  "supported": false,
  "reason": "Explain the missing data."
}}

Never invent tables, columns, business rules, or data.
Do not generate SQL for unsupported questions.
"""

    async with httpx.AsyncClient(timeout=settings.ollama_timeout_seconds) as client:
        response = await client.post(
            f"{settings.ollama_url.rstrip('/')}/api/chat",
            json={
                "model": settings.ollama_model,
                "stream": False,
                "think": False,
                "format": "json",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question},
                ],
                "options": {
                    "temperature": 0,
                    "num_predict": settings.ollama_num_predict,
                },
            },
        )

        response.raise_for_status()
        content = response.json()["message"]["content"]

    result = json.loads(content)

    if not isinstance(result, dict):
        raise ValueError("Invalid LLM output")

    return result
