import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from app.core.config import settings
from app.routes.analytics_ai import AnalyticsRequest, analytics_query
from app.text2sql.generator import generate_sql
from app.text2sql.validator import validate_sql


class ValidatorTests(unittest.TestCase):
    def test_supported_customer_aggregations(self):
        for expression, group in [
            ("customer_segment, COUNT(*) AS customers", "customer_segment"),
            ("customer_segment, ROUND(AVG(churn_probability)::numeric, 2) AS churn", "customer_segment"),
            ("country, SUM(total_revenue) AS revenue", "country"),
            ("customer_segment, AVG(total_orders) AS orders", "customer_segment"),
        ]:
            with self.subTest(expression=expression):
                sql = validate_sql(
                    f"SELECT {expression} FROM ai_reporting.customer_metrics GROUP BY {group}"
                )
                self.assertIn("LIMIT 100", sql)

    def test_preserves_smaller_limit(self):
        self.assertTrue(validate_sql(
            "SELECT country FROM ai_reporting.customer_metrics LIMIT 10"
        ).endswith("LIMIT 10"))

    def test_rejects_unsafe_sql(self):
        for sql in [
            "SELECT * FROM ai_reporting.customer_metrics",
            "SELECT country FROM serving.customer_360",
            "SELECT pg_sleep(10) FROM ai_reporting.customer_metrics",
            "DELETE FROM ai_reporting.customer_metrics",
        ]:
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                validate_sql(sql)


class GenerationTests(unittest.IsolatedAsyncioTestCase):
    async def test_generation_uses_configured_timeout_and_disables_thinking(self):
        client = AsyncMock()
        client.post.return_value = httpx.Response(
            200, json={"message": {"content": '{"supported": false, "sql": ""}'}},
            request=httpx.Request("POST", "http://ollama/api/chat"),
        )
        with patch("app.text2sql.generator.httpx.AsyncClient") as factory:
            factory.return_value.__aenter__.return_value = client
            await generate_sql("Show the top 10 products by revenue last month.")
        factory.assert_called_once_with(timeout=settings.ollama_timeout_seconds)
        payload = client.post.call_args.kwargs["json"]
        self.assertIs(payload["think"], False)
        self.assertEqual(payload["options"]["num_predict"], settings.ollama_num_predict)

    async def test_timeout_has_actionable_response(self):
        with patch("app.routes.analytics_ai.answer_analytics_question",
                   side_effect=httpx.ReadTimeout("timeout")):
            with self.assertRaises(HTTPException) as raised:
                await analytics_query(AnalyticsRequest(question="How many customers?"))
        self.assertEqual(raised.exception.status_code, 504)
        self.assertIn("OLLAMA_TIMEOUT_SECONDS", raised.exception.detail)


if __name__ == "__main__":
    unittest.main()
