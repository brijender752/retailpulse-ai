from __future__ import annotations

import httpx

from app.core.config import settings


class OllamaClient:

    def __init__(self):

        self.base_url = (
            settings.ollama_url.rstrip("/")
        )

        self.model = (
            settings.ollama_model
        )

    async def chat(
        self,
        messages: list[dict],
    ) -> str:

        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "think": False,
            "options": {
                "temperature": 0.2,
                "num_predict": settings.ollama_num_predict,
            },
        }

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(
                settings.ollama_timeout_seconds,
                connect=10.0,
            )
        ) as client:

            response = await client.post(
                f"{self.base_url}/api/chat",
                json=payload,
            )

            response.raise_for_status()

            data = response.json()

        answer = (
            data.get(
                "message",
                {},
            )
            .get(
                "content",
                "",
            )
            .strip()
        )

        if not answer:
            raise ValueError("Local LLM returned an empty answer.")

        return answer


ollama = OllamaClient()
