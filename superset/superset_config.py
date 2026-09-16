import os

from cachelib.redis import RedisCache
from pyhive.sqlalchemy_hive import HiveDialect
from sqlalchemy import text


def _get_hive_table_names(self, connection, schema=None, **kw):
    # Spark SHOW TABLES returns (namespace, tableName, isTemporary), whereas
    # Hive returns a single name column. PyHive 0.7.0 always reads column 0.
    query = "SHOW TABLES"
    if schema:
        query += " IN " + self.identifier_preparer.quote_identifier(schema)
    result = connection.execute(text(query))
    columns = [name.lower() for name in result.keys()]
    name_index = columns.index("tablename") if "tablename" in columns else 0
    return [row[name_index] for row in result]


HiveDialect.get_table_names = _get_hive_table_names


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]


SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.getenv('SUPERSET_DB_USER', 'superset')}:"
    f"{os.getenv('SUPERSET_DB_PASSWORD', 'superset')}@"
    f"{os.getenv('SUPERSET_DB_HOST', 'superset-db')}:"
    f"{os.getenv('SUPERSET_DB_PORT', '5432')}/"
    f"{os.getenv('SUPERSET_DB_NAME', 'superset')}"
)


WTF_CSRF_ENABLED = True

TALISMAN_ENABLED = False


FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}


CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": "superset-redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 1,
}


DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": "superset-redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 2,
}


RESULTS_BACKEND = RedisCache(
    host="superset-redis",
    port=6379,
    db=3,
    key_prefix="superset_results_",
)


ROW_LIMIT = 100000

SQLLAB_CTAS_NO_LIMIT = True

SUPERSET_WEBSERVER_TIMEOUT = 120
