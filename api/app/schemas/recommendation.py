from datetime import datetime

from pydantic import BaseModel


class Recommendation(BaseModel):

    product_id: int

    rank: int

    score: float | None = None

    reason: str | None = None

    model_name: str | None = None

    model_version: str | None = None

    generated_at: datetime | None = None