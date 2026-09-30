from fastapi import (
    APIRouter,
    HTTPException,
)

import httpx

from app.ai.orchestrator import (
    answer_customer_question,
)

from app.schemas.ai import (
    CustomerAIRequest,
    CustomerAIResponse,
)


router = APIRouter(
    prefix="/ai",
    tags=["GenAI"],
)


@router.post(
    "/customer",
    response_model=CustomerAIResponse,
)
async def customer_ai(
    request: CustomerAIRequest,
):

    try:

        return await answer_customer_question(
            customer_id=(
                request.customer_id
            ),
            question=(
                request.question
            ),
        )

    except httpx.ConnectError:

        raise HTTPException(
            status_code=503,
            detail=(
                "Local LLM service is unavailable."
            ),
        )

    except httpx.TimeoutException:

        raise HTTPException(
            status_code=504,
            detail=(
                "Local LLM request timed out."
            ),
        )

    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "Local LLM service rejected the request "
                f"(HTTP {exc.response.status_code}). Check that the configured "
                "model is installed and the Ollama service is healthy."
            ),
        ) from exc

    except (httpx.RequestError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail="Local LLM service returned an invalid or incomplete response.",
        ) from exc
