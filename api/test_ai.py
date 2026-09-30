import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.ai.llm import OllamaClient
from app.ai.orchestrator import answer_customer_question


class CustomerAITests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_recommendations_do_not_wait_for_llm(self):
        context = {"found": True, "recommendations": [
            {"rank": 1, "product_id": 935, "reason": "hybrid_personalized"}
        ]}
        with patch("app.ai.orchestrator.get_customer_intelligence",
                   return_value=context), patch(
            "app.ai.orchestrator.ollama.chat", new_callable=AsyncMock
        ) as chat:
            result = await answer_customer_question(
                1001, "What products should we recommend to this customer and why?"
            )
        chat.assert_not_awaited()
        self.assertIn("Product 935", result["answer"])
        self.assertIsNone(result["model"])

    async def test_generation_is_bounded_and_thinking_disabled(self):
        client = AsyncMock()
        client.post.return_value = httpx.Response(
            200,
            json={"message": {"content": " Recommend product 935. "}},
            request=httpx.Request("POST", "http://ollama/api/chat"),
        )
        with patch("app.ai.llm.httpx.AsyncClient") as factory:
            factory.return_value.__aenter__.return_value = client
            answer = await OllamaClient().chat([])
        payload = client.post.call_args.kwargs["json"]
        self.assertIs(payload["think"], False)
        self.assertGreater(payload["options"]["num_predict"], 0)
        self.assertEqual(answer, "Recommend product 935.")

    async def test_empty_generation_is_not_a_success(self):
        client = AsyncMock()
        client.post.return_value = httpx.Response(
            200, json={"message": {"content": ""}},
            request=httpx.Request("POST", "http://ollama/api/chat"),
        )
        with patch("app.ai.llm.httpx.AsyncClient") as factory:
            factory.return_value.__aenter__.return_value = client
            with self.assertRaises(ValueError):
                await OllamaClient().chat([])

    async def test_missing_customer_skips_generation(self):
        with patch("app.ai.orchestrator.get_customer_intelligence",
                   return_value={"found": False}), patch(
            "app.ai.orchestrator.ollama.chat", new_callable=AsyncMock
        ) as chat:
            result = await answer_customer_question(1001, "Recommend products")
        chat.assert_not_awaited()
        self.assertIsNone(result["model"])


if __name__ == "__main__":
    unittest.main()
