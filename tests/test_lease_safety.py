"""
Regression tests for lease-ownership and lease-lifetime bugs: a worker
writing terminal status over a job it no longer owns, a reaper sweep
splitting mark_failed's two writes, a hung task
holding its lease forever, a job that repeatedly kills its worker
retrying forever, and claim_next_job leaving an open transaction.
"""
from time import sleep

from durable_queue import jobs
from durable_queue.db import ReconnectingConnection, get_connection
from durable_queue.jobs import (
    claim_jobs,
    claim_next_job,
    enqueue,
    get_job,
    mark_failed,
    mark_succeeded,
    reap_expired_jobs,
)
from durable_queue.worker import _Heartbeat


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


def test_mark_failed_holds_the_job_against_a_sweep_between_its_updates(conn, worker_id, monkeypatch):
    """
    mark_failed is two UPDATEs, and workers run in autocommit. As two
    separate commits, a reaper sweep could land between them - a job
    past its execution ceiling has an expired lease - and hand the job
    to another worker, whose run the second UPDATE would then overwrite.
    compute_backoff runs in exactly that gap, so it stands in for the
    badly timed sweep.
    """
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id, lease_seconds=-1)  # lease already expired

    worker_conn = get_connection()
    worker_conn.autocommit = True  # as run_worker's slots are
    rival_conn = get_connection()
    rival_claims = []

    def sweep_in_the_gap(attempts: int) -> float:
        reap_expired_jobs(rival_conn)
        rival_claims.append(claim_next_job(rival_conn, "rival-worker"))
        return 0.0

    monkeypatch.setattr(jobs, "compute_backoff", sweep_in_the_gap)
    try:
        assert mark_failed(worker_conn, job_id, worker_id, "boom") is True
    finally:
        worker_conn.close()
        rival_conn.close()

    assert rival_claims == [None]
    job = get_job(conn, job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 1
    assert job["recoveries"] == 0  # the sweep skipped the locked row


def test_heartbeat_stops_extending_a_job_past_its_execution_ceiling(conn, worker_id):
    """
    Without a ceiling the heartbeat props up a hung task forever and the
    reaper can never recover it. The deadline is per job, so a hung job
    must stop being extended without affecting the others this worker
    holds.
    """
    hung_id = enqueue(conn, "some_task", {})
    healthy_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_jobs(conn, worker_id, lease_seconds=60, batch_size=2)

    heartbeat_conn = ReconnectingConnection()
    heartbeat = _Heartbeat(heartbeat_conn, worker_id, lease_seconds=60, interval=0.05)
    heartbeat.start()
    try:
        heartbeat.hold([hung_id], max_execution_seconds=0.1)  # expires almost at once
        heartbeat.hold([healthy_id])  # no deadline: extended indefinitely
        sleep(0.5)

        hung_before = get_job(conn, hung_id)["locked_until"]
        healthy_before = get_job(conn, healthy_id)["locked_until"]
        sleep(0.5)

        assert get_job(conn, hung_id)["locked_until"] == hung_before
        assert get_job(conn, healthy_id)["locked_until"] > healthy_before
    finally:
        heartbeat.stop()
        heartbeat_conn.close()


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
