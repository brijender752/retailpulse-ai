import asyncio
from typing import Any

from app.ai.tools import get_customer_intelligence
from app.db.postgres import get_connection
from app.rag.retrieval import retrieve_context
from app.text2sql.service import answer_analytics_question


def get_churn_explanation(customer_id: int) -> dict:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    prediction_date,
                    model_version,
                    output_space,
                    base_value,
                    feature_contributions
                FROM serving.churn_explanations
                WHERE customer_id = %s
                ORDER BY prediction_date DESC
                LIMIT 1
                """,
                (customer_id,),
            )
            row = cur.fetchone()

    if row is None:
        return {
            "available": False,
            "message": (
                "No computed SHAP explanation "
                "is available for this customer."
            ),
        }

    return {
        "available": True,
        "prediction_date": row[0].isoformat(),
        "model_version": row[1],
        "output_space": row[2],
        "base_value": row[3],
        "feature_contributions": row[4],
    }


TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "query_analytics",
            "description": (
                "Answer aggregate business analytics questions "
                "using the approved read-only reporting view. "
                "Not suitable for individual customer lookups."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"}
                },
                "required": ["question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_churn_explanation",
            "description": (
                "Get precomputed SHAP feature contributions "
                "for a customer's churn prediction. "
                "Use this for model-specific why questions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "integer",
                        "minimum": 1,
                    }
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_customer_intelligence",
            "description": (
                "Get Customer 360 metrics, churn prediction, "
                "and ranked recommendations for a customer."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "integer",
                        "minimum": 1,
                    }
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_retailpulse_knowledge",
            "description": (
                "Search RetailPulse documentation for "
                "business definitions, architecture, "
                "churn methodology and recommendation logic."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                    }
                },
                "required": ["question"],
            },
        },
    },
]


async def execute_tool(
    name: str,
    arguments: dict[str, Any],
) -> dict:

    if name == "query_analytics":
        question = arguments.get("question")

        if (
            not isinstance(question, str)
            or not 5 <= len(question) <= 1000
        ):
            return {"error": "Invalid analytics question"}

        return await answer_analytics_question(question)

    if name == "get_churn_explanation":
        customer_id = arguments.get("customer_id")

        if type(customer_id) is not int or customer_id <= 0:
            return {"error": "Invalid customer_id"}

        return await asyncio.to_thread(
            get_churn_explanation,
            customer_id,
        )

    if name == "get_customer_intelligence":
        customer_id = arguments.get("customer_id")

        if (
            type(customer_id) is not int
            or customer_id <= 0
        ):
            return {"error": "Invalid customer_id"}

        return await asyncio.to_thread(
            get_customer_intelligence,
            customer_id,
        )

    if name == "search_retailpulse_knowledge":
        question = arguments.get("question")

        if (
            not isinstance(question, str)
            or not question.strip()
            or len(question) > 1000
        ):
            return {"error": "Invalid search question"}

        results = await retrieve_context(
            question,
            limit=2,
        )

        return {
            "results": [
                {
                    "source": item.get("source"),
                    "heading": item.get("heading"),
                    "text": (item.get("text") or "")[:800],
                    "truncated": len(item.get("text") or "") > 800,
                    "score": item.get("score"),
                }
                for item in results
            ]
        }

    return {"error": "Tool not allowed"}
