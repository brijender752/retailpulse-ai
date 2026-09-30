from fastapi import FastAPI

from app.core.config import settings
from app.routes import ai

from app.routes import (
    churn,
    customer360,
    customers,
    health,
    recommendations,
)


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Serving API for the RetailPulse-AI "
        "data and machine-learning platform."
    ),
)


app.include_router(
    health.router
)

app.include_router(
    customers.router
)

app.include_router(
    customer360.router
)

app.include_router(
    churn.router
)

app.include_router(
    recommendations.router
)

app.include_router(
    ai.router
)

@app.get("/")
def root():

    return {
        "service": "RetailPulse AI",
        "version": settings.app_version,
        "docs": "/docs",
    }


    