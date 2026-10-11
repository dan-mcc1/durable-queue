"""
Real worker and scheduler subprocesses having their connections
dropped server-side, the way Neon drops them on a compute restart or
when it scales to zero. Before reconnecting, the worker either exited,
silently lost its heartbeat, or - once every slot had died recording a
failure - sat holding claimed jobs it would never run.
"""
import subprocess
import sys
import time
from pathlib import Path

from durable_queue.jobs import enqueue, get_job
from durable_queue.scheduler import register_schedule

REPO_ROOT = Path(__file__).resolve().parents[2]


def _spawn(mode: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "tests.chaos.reconnect_procs", mode],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for(conn, query: str, params: tuple, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        with conn.cursor() as cur:
            cur.execute(query, params)
            done = cur.fetchone()["done"]
        conn.commit()
        if done:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"still waiting after {timeout}s: {query.strip()}")
        time.sleep(0.05)


def _wait_until_succeeded(conn, job_ids: list[int], *, timeout: float) -> None:
    _wait_for(
        conn,
        "SELECT count(*) = %s AS done FROM jobs WHERE id = ANY(%s) AND status = 'succeeded'",
        (len(job_ids), job_ids),
        timeout=timeout,
    )


def _drop_every_other_connection(conn):
    """Terminate every session but ours, returning when it happened."""
    with conn.cursor() as cur:
        cur.execute("SELECT clock_timestamp() AS t")
        dropped_at = cur.fetchone()["t"]
        cur.execute(
            """
            SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity
            WHERE datname = current_database() AND pid <> pg_backend_pid()
              AND backend_type = 'client backend'
            """
        )
    conn.commit()
    return dropped_at


def test_a_worker_carries_on_after_every_connection_it_holds_is_dropped(conn):
    worker = _spawn("listening_worker")
    try:
        first = enqueue(conn, "sleep_task", {"seconds": 0})
        conn.commit()
        _wait_until_succeeded(conn, [first], timeout=15)

        dropped_at = _drop_every_other_connection(conn)
        # A claim from a session opened since the drop: the worker has
        # reconnected, and its LISTEN - replayed before any claim - is
        # back in place.
        _wait_for(
            conn,
            """
            SELECT count(*) > 0 AS done FROM pg_stat_activity
            WHERE backend_start > %s AND pid <> pg_backend_pid()
              AND query LIKE '%%SKIP LOCKED%%'
            """,
            (dropped_at,),
            timeout=15,
        )

        second = enqueue(conn, "sleep_task", {"seconds": 0})
        conn.commit()
        # Well inside the 60s poll interval, so the restored LISTEN is
        # what woke it.
        _wait_until_succeeded(conn, [second], timeout=5)
        # The pool's idle connection died too, and was checked and
        # replaced before being lent out rather than failing the job.
        assert get_job(conn, second)["attempts"] == 0
        assert worker.poll() is None
    finally:
        worker.kill()
        worker.wait()


def test_a_slot_survives_failing_to_record_a_job(conn):
    """
    One slot, and the first job it runs can't have its completion or
    its failure recorded. That slot used to die, leaving the worker
    claiming jobs with nothing to run them.
    """
    job_ids = [
        enqueue(conn, "sleep_task", {"seconds": 0}),
        enqueue(conn, "sleep_task", {"seconds": 0}),
    ]
    conn.commit()

    worker = _spawn("unrecordable_first_job")
    try:
        # That job's lease lapses and it's reaped and run again. Which
        # job it was isn't fixed: a batch claim's RETURNING doesn't
        # come back in queue order.
        _wait_until_succeeded(conn, job_ids, timeout=20)
        assert sorted(get_job(conn, job_id)["recoveries"] for job_id in job_ids) == [0, 1]
        assert worker.poll() is None
    finally:
        worker.kill()
        worker.wait()


def test_a_scheduler_carries_on_after_its_connection_is_dropped(conn):
    scheduler = _spawn("scheduler")
    try:
        register_schedule(conn, "before", "sleep_task", {"seconds": 0}, 3600)
        _wait_for(conn, "SELECT count(*) = 1 AS done FROM jobs", (), timeout=15)

        _drop_every_other_connection(conn)
        register_schedule(conn, "after", "sleep_task", {"seconds": 0}, 3600)
        _wait_for(conn, "SELECT count(*) = 2 AS done FROM jobs", (), timeout=15)
        assert scheduler.poll() is None
    finally:
        scheduler.kill()
        scheduler.wait()
