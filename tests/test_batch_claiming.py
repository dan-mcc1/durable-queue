"""
Batch claiming: a worker claims several jobs per round trip but still
runs them one at a time. The new failure mode worth proving is a worker
dying while holding a batch - every job in it must come back, not just
the one that was running.
"""
import threading
from datetime import datetime, timedelta, timezone

from durable_queue.db import get_connection
from durable_queue.jobs import (
    claim_jobs,
    enqueue,
    extend_leases,
    get_job,
    reap_expired_jobs,
)
from durable_queue.worker import generate_worker_id


def test_claim_jobs_returns_up_to_batch_size(conn, worker_id):
    for _ in range(5):
        enqueue(conn, "some_task", {})
    conn.commit()

    claimed = claim_jobs(conn, worker_id, batch_size=3)

    assert len(claimed) == 3
    for job in claimed:
        row = get_job(conn, job["id"])
        assert row["status"] == "running"
        assert row["locked_by"] == worker_id


def test_claim_jobs_returns_fewer_when_queue_is_short(conn, worker_id):
    enqueue(conn, "some_task", {})
    conn.commit()

    assert len(claim_jobs(conn, worker_id, batch_size=10)) == 1


def test_claim_jobs_returns_empty_list_when_queue_is_empty(conn, worker_id):
    assert claim_jobs(conn, worker_id, batch_size=10) == []


def test_claim_jobs_does_not_claim_jobs_scheduled_for_later(conn, worker_id):
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO jobs (task, status, run_at) VALUES ('t', 'pending', %s)", (future,))
    conn.commit()

    assert claim_jobs(conn, worker_id, batch_size=10) == []


def test_extend_leases_covers_every_held_job(conn, worker_id):
    for _ in range(3):
        enqueue(conn, "some_task", {})
    conn.commit()
    claimed = claim_jobs(conn, worker_id, lease_seconds=5, batch_size=3)
    before = {job["id"]: get_job(conn, job["id"])["locked_until"] for job in claimed}

    extend_leases(conn, [job["id"] for job in claimed], worker_id, lease_seconds=600)

    for job_id, previous in before.items():
        assert get_job(conn, job_id)["locked_until"] > previous


def test_extend_leases_ignores_jobs_held_by_another_worker(conn, worker_id):
    enqueue(conn, "some_task", {})
    conn.commit()
    claimed = claim_jobs(conn, worker_id, lease_seconds=60, batch_size=1)
    job_id = claimed[0]["id"]
    before = get_job(conn, job_id)["locked_until"]

    extend_leases(conn, [job_id], "a-different-worker", lease_seconds=999)

    assert get_job(conn, job_id)["locked_until"] == before


def test_a_whole_abandoned_batch_is_recovered_not_just_the_running_job(conn, worker_id):
    """
    The failure mode batching introduces: a worker holding ten leases
    dies, and all ten must return to pending - not only whichever one
    it happened to be executing.
    """
    for _ in range(5):
        enqueue(conn, "some_task", {})
    conn.commit()
    claimed = claim_jobs(conn, worker_id, lease_seconds=-1, batch_size=5)
    assert len(claimed) == 5

    assert reap_expired_jobs(conn) == 5

    for job in claimed:
        row = get_job(conn, job["id"])
        assert row["status"] == "pending"
        assert row["locked_by"] is None


def test_batch_claiming_never_hands_the_same_job_to_two_workers(conn):
    """
    The no-double-claim guarantee, re-proven for the batched claim: the
    subquery still uses FOR UPDATE SKIP LOCKED, but selects many rows at
    once, so it's worth confirming nothing overlaps.
    """
    job_count = 60
    thread_count = 6

    job_ids = [enqueue(conn, "some_task", {}) for _ in range(job_count)]
    conn.commit()

    claimed: list[dict] = []
    lock = threading.Lock()

    def claim_until_empty() -> None:
        worker_conn = get_connection()
        this_worker = generate_worker_id()
        try:
            while True:
                batch = claim_jobs(worker_conn, this_worker, batch_size=7)
                if not batch:
                    break
                with lock:
                    claimed.extend(batch)
        finally:
            worker_conn.close()

    threads = [threading.Thread(target=claim_until_empty) for _ in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    claimed_ids = [job["id"] for job in claimed]
    assert sorted(claimed_ids) == sorted(job_ids)
    assert len(claimed_ids) == len(set(claimed_ids))
