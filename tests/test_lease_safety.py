"""
Regression tests for lease-ownership and lease-lifetime bugs: a worker
writing terminal status over a job it no longer owns, a hung task
holding its lease forever, a job that repeatedly kills its worker
retrying forever, and claim_next_job leaving an open transaction.
"""
import threading
from time import monotonic

from durable_queue.db import get_connection
from durable_queue.jobs import (
    claim_next_job,
    enqueue,
    get_job,
    mark_failed,
    mark_succeeded,
    reap_expired_jobs,
)
from durable_queue.worker import _heartbeat_loop


def test_claim_next_job_does_not_leave_an_open_transaction(conn, worker_id):
    """
    An idle worker used to sit in "idle in transaction" indefinitely -
    the claim UPDATE matched no rows and was never committed. That pins
    Postgres's xmin horizon and blocks autovacuum database-wide.
    """
    assert claim_next_job(conn, worker_id) is None

    checker = get_connection()
    try:
        with checker.cursor() as cur:
            cur.execute(
                "SELECT state FROM pg_stat_activity WHERE pid = %s",
                (conn.info.backend_pid,),
            )
            state = cur.fetchone()["state"]
    finally:
        checker.close()

    assert state != "idle in transaction"


def test_mark_succeeded_refuses_a_job_this_worker_no_longer_owns(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id)

    assert mark_succeeded(conn, job_id, "a-different-worker") is False
    assert get_job(conn, job_id)["status"] == "running"

    assert mark_succeeded(conn, job_id, worker_id) is True
    assert get_job(conn, job_id)["status"] == "succeeded"


def test_mark_failed_refuses_a_job_this_worker_no_longer_owns(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id)

    assert mark_failed(conn, job_id, "a-different-worker", "boom") is False

    job = get_job(conn, job_id)
    assert job["status"] == "running"
    assert job["attempts"] == 0
    assert job["last_error"] is None


def test_heartbeat_stops_extending_past_the_execution_ceiling(conn, worker_id):
    """
    Without a ceiling the heartbeat props up a hung task forever, so the
    reaper can never recover it. This loop must return on its own even
    though stop_event is never set.
    """
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id, lease_seconds=60)

    never_set = threading.Event()
    started = monotonic()
    _heartbeat_loop(
        conn, job_id, worker_id, 60, 0.05, never_set, max_execution_seconds=0.2
    )

    assert monotonic() - started < 5.0


def test_reap_dead_letters_a_job_that_keeps_killing_its_worker(conn, worker_id):
    """
    A poison pill crashes the worker before mark_failed can run, so
    attempts never increments and max_attempts can never stop it.
    Recoveries are what bound it.
    """
    job_id = enqueue(conn, "some_task", {})
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET max_recoveries = 2 WHERE id = %s", (job_id,))
    conn.commit()

    # A negative lease claims the job already expired, standing in for a
    # worker that died the instant it picked the job up.
    claim_next_job(conn, worker_id, lease_seconds=-1)
    assert reap_expired_jobs(conn) == 1
    job = get_job(conn, job_id)
    assert job["status"] == "pending"
    assert job["recoveries"] == 1
    assert job["attempts"] == 0  # a crash is not a failed attempt

    claim_next_job(conn, worker_id, lease_seconds=-1)
    assert reap_expired_jobs(conn) == 1
    job = get_job(conn, job_id)
    assert job["status"] == "dead"
    assert job["recoveries"] == 2
    assert "repeatedly died" in job["last_error"]
    assert job["finished_at"] is not None
