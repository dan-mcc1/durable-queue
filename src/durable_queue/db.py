import os
import psycopg
from psycopg.rows import dict_row


def get_dsn() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgres://durable_queue:durable_queue@localhost:5432/durable_queue_dev"
        )


def get_connection() -> psycopg.Connection:
    return psycopg.connect(get_dsn(), row_factory=dict_row)
