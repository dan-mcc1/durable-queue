import os
import psycopg
from psycopg.rows import dict_row

def get_connection() -> psycopg.Connection:
    dsn = os.environ.get(
        "DATABASE_URL", 
        "postgres://durable_queue:durable_queue@localhost:5432/durable_queue_dev"
        )
    return psycopg.connect(dsn, row_factory=dict_row)