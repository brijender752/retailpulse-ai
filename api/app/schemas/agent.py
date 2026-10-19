from uuid import UUID
from pydantic import BaseModel, Field


class AgentRequest(BaseModel):
    question: str = Field(
        min_length=3,
        max_length=1000,
    )
    session_id: UUID | None = None


class AgentResponse(BaseModel):
    session_id: UUID
    answer: str
    tools_used: list[dict]