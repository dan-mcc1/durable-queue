import pytest

from durable_queue.db import get_connection


@pytest.fixture
def conn():
    connection = get_connection()
    with connection.cursor() as cur:
        cur.execute(
            "TRUNCATE jobs, effects, schedules, chaos_observations,"
            " problems, chunk_results, bench_samples"
        )
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture
def worker_id() -> str:
    return "test-worker"