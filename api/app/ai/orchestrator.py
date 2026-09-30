from __future__ import annotations

import json
import re

from starlette.concurrency import run_in_threadpool

from app.ai.llm import ollama
from app.ai.prompts import SYSTEM_PROMPT
from app.ai.tools import (
    get_customer_intelligence,
)


def is_direct_recommendation_question(question: str) -> bool:
    normalized = " ".join(question.lower().split()).rstrip("?.!")
    return bool(re.fullmatch(
        r"(?:what|which) products should (?:we|i) recommend "
        r"to (?:this|the) customer(?: and why)?", normalized
    ))


def recommendation_answer(context: dict) -> str:
    recommendations = context.get("recommendations", [])[:5]
    if not recommendations:
        return "No product recommendations are available for this customer."
    reasons = {
        "hybrid_personalized": "selected by the hybrid personalized recommendation model",
        "personalized_similarity": "selected based on personalized similarity",
        "popularity": "selected based on product popularity",
    }
    lines = ["Top recommended products based on the stored model rankings:"]
    for item in recommendations:
        reason = item.get("reason")
        explanation = reasons.get(reason, f"recorded reason: {reason}" if reason else
                                  "no recommendation reason is available")
        lines.append(f"{item['rank']}. Product {item['product_id']}: {explanation}.")
    lines.append("Product names and more detailed explanations are unavailable in this context.")
    return "\n".join(lines)


async def answer_customer_question(
    customer_id: int,
    question: str,
) -> dict:

    context = await run_in_threadpool(
        get_customer_intelligence, customer_id
    )

    if not context.get(
        "found"
    ):

        return {
            "customer_id": customer_id,
            "question": question,
            "answer": (
                f"Customer {customer_id} "
                "was not found."
            ),
            "model": None,
        }

    if is_direct_recommendation_question(question):
        return {
            "customer_id": customer_id,
            "question": question,
            "answer": recommendation_answer(context),
            "context_used": {
                "customer_360": True,
                "churn_prediction": False,
                "recommendations": bool(context.get("recommendations")),
            },
            "model": None,
        }

    context_json = json.dumps(
        context,
        separators=(",", ":"),
        default=str,
    )

    user_prompt = f"""
Customer ID:
{customer_id}

RetailPulse context:

{context_json}

User question:

{question}

Answer using only the supplied RetailPulse context for
customer-specific facts.

Answer the specific question directly in at most 180 words.
For product recommendations, list the top 5 supplied products in rank order
with their supplied reasons. Include product IDs. Do not add unrelated
customer overview or churn sections unless the question asks for them.

Do not invent missing information.
"""

    messages = [
        {
            "role": "system",
            "content": SYSTEM_PROMPT,
        },
        {
            "role": "user",
            "content": user_prompt,
        },
    ]

    answer = await ollama.chat(
        messages
    )

    return {
        "customer_id": customer_id,
        "question": question,
        "answer": answer,
        "context_used": {
            "customer_360": True,
            "churn_prediction": (
                context.get("churn")
                is not None
            ),
            "recommendations": (
                len(
                    context.get(
                        "recommendations",
                        [],
                    )
                )
                > 0
            ),
        },
        "model": ollama.model,
    }
