"""A live Flink directory must be frozen before bootstrap count/write actions."""
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, patch, call


class BootstrapTests(unittest.TestCase):
    def test_count_and_write_use_fixed_file_manifest(self):
        session = types.ModuleType("iceberg_session")
        session.create_iceberg_spark_session = MagicMock()
        path = Path(__file__).resolve().parents[1] / "lakehouse/iceberg/jobs/bootstrap_minio_to_iceberg.py"
        spec = importlib.util.spec_from_file_location("bootstrap_under_test", path)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"iceberg_session": session, spec.name: module}):
            spec.loader.exec_module(module)
            spark = MagicMock()
            reader = spark.read
            reader.option.return_value = reader
            discovered, frozen = MagicMock(), MagicMock()
            files = ["s3a://retailpulse/streaming/bronze/orders/bucket/part.parquet"]
            discovered.inputFiles.return_value = files
            reader.parquet.side_effect = [discovered, frozen]
            frozen.count.return_value = 3
            spark.table.return_value.count.return_value = 3
            dataset = module.Dataset("orders", "s3a://retailpulse/streaming/bronze/orders/", "retailpulse.bronze.orders")
            with patch.object(module, "source_exists", return_value=True):
                module.migrate_one(spark, dataset)
            self.assertEqual(reader.parquet.call_args_list, [call(dataset.source), call(*files)])
            discovered.count.assert_not_called()
            discovered.writeTo.assert_not_called()
            frozen.writeTo.assert_called_once_with(dataset.target)


if __name__ == "__main__":
    unittest.main()
