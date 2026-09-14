"""Readiness must require every entity and must preserve existing connectors."""
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


class StreamingSetupTests(unittest.TestCase):
    def setUp(self):
        config = types.ModuleType("retailpulse.config")
        config.POSTGRES = {}
        config.DEBEZIUM_URL = "http://debezium:8083"
        config.DEBEZIUM_CONNECTOR = "test"
        config.MINIO_BUCKET = "retailpulse"
        health = types.ModuleType("retailpulse.health")
        health.get_minio_client = MagicMock()
        self.client = health.get_minio_client.return_value
        self.requests = MagicMock()
        with patch.dict(sys.modules, {"retailpulse.config": config, "retailpulse.health": health,
                                      "requests": self.requests, "psycopg2": MagicMock()}):
            path = Path(__file__).resolve().parents[1] / "airflow/include/retailpulse/streaming_setup.py"
            spec = importlib.util.spec_from_file_location("streaming_setup", path)
            self.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.module)

    def test_pending_and_empty_files_are_not_ready(self):
        self.client.list_objects.return_value = [
            types.SimpleNamespace(object_name="part.parquet.inprogress", size=100),
            types.SimpleNamespace(object_name="part.parquet", size=0),
        ]
        self.assertFalse(self.module.bronze_files_ready())

    def test_every_entity_must_have_a_committed_file(self):
        complete = [types.SimpleNamespace(object_name="bucket/part.parquet", size=100)]
        self.client.list_objects.side_effect = lambda bucket, prefix, recursive: [] if "/payments/" in prefix else complete
        self.assertFalse(self.module.bronze_files_ready())
        self.client.list_objects.side_effect = None
        self.client.list_objects.return_value = complete
        self.assertTrue(self.module.bronze_files_ready())

    def test_existing_connector_is_not_replaced(self):
        self.requests.get.return_value.status_code = 200
        self.module.ensure_connector()
        self.requests.post.assert_not_called()
        self.requests.put.assert_not_called()

    def test_populated_source_is_reused(self):
        self.module.PROJECT = Path(__file__).resolve().parents[1]
        cursor = self.module.psycopg2.connect.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (10,)
        self.assertEqual(self.module.initialize_source()["source"], "existing")
        self.module.psycopg2.connect.return_value.close.assert_called_once()

    def test_partial_source_fails_before_seeding(self):
        self.module.PROJECT = Path(__file__).resolve().parents[1]
        cursor = self.module.psycopg2.connect.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [(1,)] + [(0,)] * 8
        with self.assertRaisesRegex(RuntimeError, "partially populated"):
            self.module.initialize_source()

    def test_connector_server_error_does_not_create_replacement(self):
        self.requests.get.return_value.status_code = 500
        self.requests.get.return_value.raise_for_status.side_effect = RuntimeError("server error")
        with self.assertRaisesRegex(RuntimeError, "server error"):
            self.module.ensure_connector()
        self.requests.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
