from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import httpx
import psycopg2
import logging

from app.text2sql.service import answer_analytics_question

logger = logging.getLogger(__name__)


router = APIRouter(
    prefix="/analytics-ai",
    tags=["AI Analytics"],
)


class AnalyticsRequest(BaseModel):
    question: str = Field(
        min_length=5,
        max_length=1000,
    )


@router.post("/query")
async def analytics_query(request: AnalyticsRequest):
    try:
        return await answer_analytics_question(
            request.question
        )

    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        )

    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail=(
                "SQL generation timed out. Check that Ollama has loaded "
                "OLLAMA_MODEL or increase OLLAMA_TIMEOUT_SECONDS."
            ),
        ) from exc

    except httpx.HTTPStatusError as exc:
        logger.warning("Analytics LLM returned HTTP %s", exc.response.status_code)
        detail = "Ollama rejected the SQL generation request. Check its server logs."
        if exc.response.status_code == 404:
            detail = "Ollama endpoint or model not found. Check OLLAMA_URL and pull OLLAMA_MODEL."
        raise HTTPException(status_code=503, detail=detail) from exc

    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "Cannot reach Ollama. Check that it is running and OLLAMA_URL "
                "is reachable from the API container."
            ),
        ) from exc

    except psycopg2.Error as exc:
        logger.warning("Analytics database request failed (SQLSTATE %s)", exc.pgcode)
        if isinstance(exc, psycopg2.OperationalError):
            raise HTTPException(
                status_code=503,
                detail=(
                    "Analytics database connection failed. Check database availability, "
                    "the analyst role, and that ANALYST_DB_PASSWORD matches its password."
                ),
            ) from exc
        if exc.pgcode in {"42P01", "3F000", "42501"}:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Analytics reporting setup is incomplete. Create "
                    "ai_reporting.customer_metrics and grant the analyst role "
                    "USAGE on ai_reporting and SELECT on the view."
                ),
            ) from exc
        raise HTTPException(
            status_code=422,
            detail="Generated SQL could not be executed",
        )
