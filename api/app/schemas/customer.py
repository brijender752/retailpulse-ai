from pydantic import BaseModel

from app.schemas.churn import (
    ChurnPrediction,
)

from app.schemas.recommendation import (
    Recommendation,
)


class Customer360(BaseModel):

    customer_id: int

    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None

    country: str | None = None
    state: str | None = None
    city: str | None = None

    customer_segment: str | None = None

    total_orders: int | None = None

    total_revenue: float | None = None

    avg_order_value: float | None = None

    days_since_last_order: int | None = None

    total_payments: int | None = None

    total_website_events: int | None = None

    total_support_tickets: int | None = None


class CustomerIntelligence(BaseModel):

    customer: Customer360

    churn: ChurnPrediction | None = None

    recommendations: list[
        Recommendation
    ] = []