from datetime import datetime

from pydantic import BaseModel


class ChurnPrediction(BaseModel):

    customer_id: int

    churn_probability: float

    churn_prediction: int | None = None

    risk_level: str | None = None

    model_version: str | None = None

    prediction_date: datetime | None = None