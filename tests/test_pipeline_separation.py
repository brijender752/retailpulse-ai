"""Offline contracts for storage isolation and batch container submission."""
import ast
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]


def constants(path):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8-sig"))
    return {n.targets[0].id: n.value.value for n in tree.body
            if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
            and isinstance(n.value, ast.Constant)}


class PipelineSeparationTests(unittest.TestCase):
    def test_batch_chain_is_isolated_and_connected(self):
        bronze = constants('spark/jobs/batch/bronze/postgres_to_bronze.py')
        silver = constants('spark/jobs/batch/silver/bronze_to_silver.py')
        gold = constants('spark/jobs/batch/gold/silver_to_gold.py')
        self.assertEqual(bronze['BRONZE_BASE_PATH'], 's3a://retailpulse/batch/bronze')
        self.assertEqual(bronze['BRONZE_BASE_PATH'], silver['BRONZE_BASE_PATH'])
        self.assertEqual(silver['SILVER_BASE_PATH'], gold['SILVER_BASE_PATH'])
        self.assertEqual(gold['GOLD_BASE_PATH'], 's3a://retailpulse/batch/gold')

    def test_stream_readers_and_writers_use_new_area(self):
        self.assertEqual(constants('flink/jobs/streaming/bronze/retailpulse_bronze.py')['MINIO_BASE'],
                         's3://retailpulse/streaming/bronze')
        for folder in ['flink/jobs/streaming', 'lakehouse/iceberg/jobs']:
            for path in (ROOT / folder).rglob('*.py'):
                source = path.read_text(encoding='utf-8-sig')
                for obsolete in ['s3://retailpulse/bronze', 's3a://retailpulse/bronze',
                                 's3://retailpulse/gold_stream', 's3://retailpulse/silver_stream']:
                    self.assertNotIn(obsolete, source, str(path))

    def test_batch_submission_environment_and_failure(self):
        docker = types.ModuleType('docker')
        docker.from_env = MagicMock()
        # DockerClient has close(), but no context-manager protocol.
        client = types.SimpleNamespace(containers=MagicMock(), close=MagicMock())
        docker.from_env.return_value = client
        container = client.containers.get.return_value
        container.exec_run.return_value = types.SimpleNamespace(exit_code=0, output=b'done')
        config = types.ModuleType('retailpulse.config')
        config.POSTGRES = dict(host='postgres', port=5432, database='retailpulse', user='test', password='test')
        config.MINIO_ENDPOINT = 'minio:9000'
        config.MINIO_ACCESS_KEY = config.MINIO_SECRET_KEY = 'test'
        iceberg = types.ModuleType('retailpulse.iceberg_control')
        iceberg.SPARK_CONTAINER = 'retailpulse-spark-iceberg'
        with patch.dict(sys.modules, {'docker': docker, 'retailpulse.config': config,
                                     'retailpulse.iceberg_control': iceberg}):
            spec = importlib.util.spec_from_file_location('batch_control', ROOT / 'airflow/include/retailpulse/batch_control.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.assertEqual(module.run_batch_job('bronze'), 'done')
            client.close.assert_called_once_with()
            client.close.reset_mock()
            args, kwargs = container.exec_run.call_args
            self.assertEqual(args[0], [
                '/opt/spark/bin/spark-submit',
                '--driver-class-path', '/opt/retailpulse/spark/jars/postgresql-42.7.12.jar',
                '--jars', '/opt/retailpulse/spark/jars/postgresql-42.7.12.jar',
                '/opt/retailpulse/spark/jobs/batch/run_batch.py', 'bronze',
            ])
            self.assertEqual(kwargs['environment']['POSTGRES_HOST'], 'postgres')
            self.assertEqual(kwargs['workdir'], '/opt/retailpulse')
            container.exec_run.return_value = types.SimpleNamespace(exit_code=1, output=b'failed')
            with self.assertRaisesRegex(RuntimeError, 'Batch silver failed'):
                module.run_batch_job('silver')
            client.close.assert_called_once_with()
            client.close.reset_mock()
            container.exec_run.side_effect = OSError('Docker connection lost')
            with self.assertRaisesRegex(OSError, 'Docker connection lost'):
                module.run_batch_job('gold')
            client.close.assert_called_once_with()
            client.close.reset_mock()
            client.containers.get.side_effect = LookupError('Container missing')
            with self.assertRaisesRegex(LookupError, 'Container missing'):
                module.run_batch_job('bronze')
            client.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
