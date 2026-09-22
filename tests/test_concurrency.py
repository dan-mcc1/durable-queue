import threading

from durable_queue.db import get_connection
from durable_queue.jobs import claim_next_job, enqueue
from durable_queue.registry import task
from durable_queue.worker import generate_worker_id


@task
def concurrency_noop() -> None:
    pass


def test_claim_next_job_never_double_claims_under_concurrency(conn):
    """
    With many workers racing for the same jobs, every job is claimed
    exactly once, never zero times, never twice. This is what FOR UPDATE
    SKIP LOCKED buys; without it, workers hand out the same row twice.
    """
    job_count = 20
    worker_count = 8

    job_ids = [enqueue(conn, "concurrency_noop", {}) for _ in range(job_count)]
    conn.commit()

    claimed: list[dict] = []
    claimed_lock = threading.Lock()

    def run_one_worker() -> None:
        worker_conn = get_connection()
        this_worker_id = generate_worker_id()
        try:
            while True:
                row = claim_next_job(worker_conn, this_worker_id)
                if row is None:
                    break
                with claimed_lock:
                    claimed.append(row)
        finally:
            worker_conn.close()

    threads = [threading.Thread(target=run_one_worker) for _ in range(worker_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    claimed_ids = [row["id"] for row in claimed]

    assert sorted(claimed_ids) == sorted(job_ids), "every job should be claimed exactly once"
    assert len(claimed_ids) == len(set(claimed_ids)), "no job should be claimed twice"
