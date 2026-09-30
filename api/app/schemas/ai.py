from pydantic import (
    BaseModel,
    Field,
)


class CustomerAIRequest(BaseModel):

    customer_id: int

    question: str = Field(
        min_length=3,
        max_length=1000,
    )


class ContextUsed(BaseModel):

    customer_360: bool

    churn_prediction: bool

    recommendations: bool


class CustomerAIResponse(BaseModel):

    customer_id: int

    question: str

    answer: str

    context_used: (
        ContextUsed
        |
        None
    ) = None

    model: str | None = None