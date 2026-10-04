"""
run_job given a pool rather than a connection, as run_worker's slots
call it: a connection is borrowed only for the database work each job
actually does, instead of every slot owning one.
"""
import pytest
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from durable_queue.db import get_dsn
from durable_queue.jobs import claim_next_job, enqueue, get_job
from durable_queue.registry import task
from durable_queue.worker import run_job

# The pool under test, for the task below to borrow from mid-run.
_pool_seen_by_task: list[ConnectionPool] = []


@task
def pooled_txn_write(conn, effect_key: str) -> None:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO chaos_observations (effect_key) VALUES (%s)", (effect_key,))


@task
def pooled_borrows_while_running() -> None:
    # The pool has exactly one connection. If run_job were holding it
    # through this call, as a slot used to hold its own, this would
    # time out.
    with _pool_seen_by_task[0].connection(timeout=1):
        pass


@task
def pooled_noop() -> None:
    pass


@pytest.fixture
def pool():
    with ConnectionPool(
        get_dsn(),
        kwargs={"row_factory": dict_row, "autocommit": True},
        min_size=1, max_size=1, open=True,
    ) as single_connection_pool:
        yield single_connection_pool


def _claim(conn, worker_id) -> dict:
    job = claim_next_job(conn, worker_id)
    assert job is not None
    return job


def test_transactional_task_commits_with_its_job_on_a_borrowed_connection(conn, worker_id, pool):
    job_id = enqueue(conn, "pooled_txn_write", {"effect_key": "pooled-1"})
    conn.commit()

    run_job(pool, _claim(conn, worker_id), worker_id)

    assert get_job(conn, job_id)["status"] == "succeeded"
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM chaos_observations WHERE effect_key = 'pooled-1'")
        assert cur.fetchone()["n"] == 1


def test_plain_task_holds_no_connection_while_it_runs(conn, worker_id, pool):
    job_id = enqueue(conn, "pooled_borrows_while_running", {})
    conn.commit()

    _pool_seen_by_task.append(pool)
    try:
        run_job(pool, _claim(conn, worker_id), worker_id)
    finally:
        _pool_seen_by_task.clear()

    job = get_job(conn, job_id)
    assert job["status"] == "succeeded", job["last_error"]


def test_a_dropped_connection_costs_one_retry_not_the_slot(conn, worker_id, pool):
    """
    A slot that owned its connection was finished once that connection
    dropped: recording the failure needed the same dead connection, so
    the exception escaped and took the slot's thread with it. From a
    pool, the broken connection is discarded and the failure is recorded
    on a fresh one, so the job retries and the slot carries on.
    """
    with pool.connection() as pooled:
        doomed_pid = pooled.info.backend_pid
    with conn.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(%s, 5000)", (doomed_pid,))
    conn.commit()

    job_id = enqueue(conn, "pooled_noop", {})
    conn.commit()

    run_job(pool, _claim(conn, worker_id), worker_id)  # must not raise
    job = get_job(conn, job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 1

    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET run_at = now() WHERE id = %s", (job_id,))
    conn.commit()
    run_job(pool, _claim(conn, worker_id), worker_id)
    assert get_job(conn, job_id)["status"] == "succeeded"
