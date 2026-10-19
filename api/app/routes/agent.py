import asyncio

import httpx

from fastapi import APIRouter, HTTPException

from app.agent.orchestrator import run_agent
from app.schemas.agent import (
    AgentRequest,
    AgentResponse,
)


router = APIRouter(
    prefix="/agent",
    tags=["AI Agent"],
)


@router.post(
    "/chat",
    response_model=AgentResponse,
)
async def chat(request: AgentRequest):
    try:
        return await asyncio.wait_for(
            run_agent(
                question=request.question,
                session_id=request.session_id,
            ),
            timeout=300.0,
        )

    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail="Agent exceeded the 300-second time limit",
        )

    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="LLM request timed out",
        )

    except httpx.HTTPError:
        raise HTTPException(
            status_code=503,
            detail="LLM service unavailable",
        )

    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        )
