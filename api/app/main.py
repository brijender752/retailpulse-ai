from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.core.config import settings
from app.routes import ai
from app.routes import agent
from app.routes import analytics_ai

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

app.include_router(agent.router)
app.include_router(analytics_ai.router)

Instrumentator().instrument(app).expose(
    app,
    endpoint="/metrics",
    include_in_schema=False,
)

@app.get("/")
def root():

    return {
        "service": "RetailPulse AI",
        "version": settings.app_version,
        "docs": "/docs",
    }



