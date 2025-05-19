from clickhouse_driver import Client

from db_connection.db_config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, \
    CLICKHOUSE_DATABASE

DB_CONNECTION_PARAMS = {
    'host': CLICKHOUSE_HOST,
    'port': CLICKHOUSE_PORT,
    'user': CLICKHOUSE_USER,
    'password': CLICKHOUSE_PASSWORD,
    'database': CLICKHOUSE_DATABASE
}


def get_db_connection():
    return Client(**DB_CONNECTION_PARAMS)
