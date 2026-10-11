"""
Idle mode, for a database that scales to zero: with nothing due, the
worker and scheduler hold no connections and so send no queries, and
they wake for whatever makes work - a schedule coming due, a retry, an
enqueue in this process. Run here through Runner, in-process, the way
an app embeds it.
"""
import time
from datetime import datetime, timedelta, timezone
from time import monotonic

import pytest

from durable_queue import wakeup
from durable_queue.jobs import enqueue, get_job
from durable_queue.registry import task
from durable_queue.runner import Runner
from durable_queue.scheduler import register_schedule

_calls: dict[str, int] = {}


@task
def idle_noop() -> None:
    pass


@task
def idle_slow(seconds: float) -> None:
    time.sleep(seconds)


@task
def idle_fails_once(key: str) -> None:
    _calls[key] = _calls.get(key, 0) + 1
    if _calls[key] == 1:
        raise RuntimeError("transient")


@pytest.fixture
def runner(conn):  # after conn, whose TRUNCATE mustn't wait on the runner
    embedded = Runner(concurrency=2)
    yield embedded
    embedded.stop()


def _wait_until(check, *, timeout: float) -> None:
    deadline = monotonic() + timeout
    while not check():
        if monotonic() > deadline:
            raise AssertionError(f"still waiting after {timeout}s")
        time.sleep(0.05)


def _status(conn, job_id: int) -> str:
    status = get_job(conn, job_id)["status"]
    conn.commit()
    return status


def _queue_connections(conn) -> int:
    """Sessions the queue has open, besides this test's own."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) AS n FROM pg_stat_activity
            WHERE application_name = 'durable-queue' AND pid <> pg_backend_pid()
            """
        )
        n = cur.fetchone()["n"]
    conn.commit()
    return n


def test_a_wake_between_looking_and_sleeping_is_not_lost():
    seen = wakeup.current()
    wakeup.wake()  # lands after the look, before the sleep

    started = monotonic()
    wakeup.wait(seen, 5.0)
    assert monotonic() - started < 1.0


def test_after_its_work_an_idle_runner_holds_no_connections(conn, runner):
    job_id = enqueue(conn, "idle_noop", {})
    conn.commit()

    runner.start()
    _wait_until(lambda: _status(conn, job_id) == "succeeded", timeout=5)
    # Nothing left due, so it sleeps with no end in sight - having let go
    # of every connection, so the database is free to sleep too.
    _wait_until(lambda: _queue_connections(conn) == 0, timeout=15)


def test_an_idle_runner_wakes_when_a_schedule_comes_due(conn, runner):
    register_schedule(
        conn, "soon", "idle_noop", {}, 3600,
        first_run_at=datetime.now(timezone.utc) + timedelta(seconds=2),
    )
    runner.start()

    def ran() -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM jobs WHERE status = 'succeeded'")
            n = cur.fetchone()["n"]
        conn.commit()
        return n == 1

    _wait_until(ran, timeout=10)
    # Next run is an hour off: asleep again, connections closed.
    _wait_until(lambda: _queue_connections(conn) == 0, timeout=15)


def test_an_idle_worker_wakes_for_a_retry(conn, runner):
    """
    The retry's run_at is set after the worker last looked at when to
    wake, so the failure itself has to wake it to look again.
    """
    job_id = enqueue(conn, "idle_fails_once", {"key": "retry-1"})
    conn.commit()
    runner.start()

    _wait_until(lambda: _status(conn, job_id) == "succeeded", timeout=10)
    assert get_job(conn, job_id)["attempts"] == 1


def test_an_idle_worker_wakes_for_a_sqlalchemy_enqueue_once_it_commits(conn, runner):
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("psycopg2")
    from sqlalchemy import create_engine, make_url
    from sqlalchemy.orm import Session

    from durable_queue import sqlalchemy as dq
    from durable_queue.db import get_dsn

    runner.start()
    # Nothing scheduled and no jobs: asleep with no timeout at all, so
    # only the commit can wake it.
    _wait_until(lambda: _queue_connections(conn) == 0, timeout=15)

    engine = create_engine(make_url(get_dsn()).set(drivername="postgresql+psycopg2"))
    try:
        with Session(engine) as session:
            job_id = dq.enqueue(session, "idle_noop", {})
            session.commit()
        _wait_until(lambda: _status(conn, job_id) == "succeeded", timeout=5)
    finally:
        engine.dispose()


def test_stopping_hands_back_unstarted_jobs_and_finishes_running_ones(conn):
    job_ids = [enqueue(conn, "idle_slow", {"seconds": 0.5}) for _ in range(4)]
    conn.commit()

    runner = Runner(concurrency=1)
    runner.start()
    try:
        # All four claimed in one batch, one of them started.
        def all_claimed() -> bool:
            return all(_status(conn, job_id) == "running" for job_id in job_ids)

        _wait_until(all_claimed, timeout=5)
        time.sleep(0.1)
    finally:
        runner.stop()

    jobs = [get_job(conn, job_id) for job_id in job_ids]
    assert sorted(job["status"] for job in jobs) == ["pending", "pending", "pending", "succeeded"]
    for job in jobs:
        if job["status"] == "pending":
            assert job["locked_by"] is None
            assert job["recoveries"] == 0  # handed back, not recovered
