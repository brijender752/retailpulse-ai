from __future__ import annotations

import os

import requests


SUPERSET_URL = os.getenv(
    "RETAILPULSE_SUPERSET_URL",
    "http://superset:8088",
)


def check_superset_health() -> dict:

    response = requests.get(
        f"{SUPERSET_URL}/health",
        timeout=15,
    )

    response.raise_for_status()

    return {
        "status_code": response.status_code,
        "body": response.text,
    }