from datetime import datetime, timezone

from durable_queue.jobs import claim_next_job, compute_backoff, enqueue, mark_failed


def test_compute_backoff_stays_within_bounds():
    for attempts in range(1, 10):
        for _ in range(20):
            delay = compute_backoff(attempts, base_seconds=1.0, max_seconds=60.0)
            assert 0 <= delay <= min(60.0, 1.0 * 2 ** attempts)


def test_compute_backoff_is_capped_at_max_seconds():
    delay = compute_backoff(20, base_seconds=1.0, max_seconds=60.0)
    assert delay <= 60.0


def test_mark_failed_reschedules_when_attempts_remain(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id)

    mark_failed(conn, job_id, worker_id, "transient boom")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, attempts, last_error, locked_by, locked_until,
                   run_at, finished_at
            FROM jobs WHERE id = %s
            """,
            (job_id,),
        )
        row = cur.fetchone()

    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["last_error"] == "transient boom"
    assert row["locked_by"] is None
    assert row["locked_until"] is None
    assert row["finished_at"] is None
    assert row["run_at"] > datetime.now(timezone.utc)


def test_mark_failed_goes_dead_after_max_attempts(conn, worker_id):
    # Start one attempt short of the limit rather than looping
    # mark_failed: each failure releases the lease, so a second call
    # without re-claiming is correctly rejected by the ownership guard.
    job_id = enqueue(conn, "some_task", {})
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET attempts = max_attempts - 1 WHERE id = %s", (job_id,))
    conn.commit()
    claim_next_job(conn, worker_id)

    assert mark_failed(conn, job_id, worker_id, "still failing") is True

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts, last_error, finished_at FROM jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()

    assert row["status"] == "dead"
    assert row["attempts"] == 5
    assert row["last_error"] == "still failing"
    assert row["finished_at"] is not None


def test_claim_next_job_does_not_claim_before_run_at(conn, worker_id):
    """
    Regression guard: without "AND run_at <= now()" in claim_next_job's
    WHERE clause, a rescheduled retry would be immediately reclaimed,
    making the whole backoff mechanism a no-op.
    """
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id)
    mark_failed(conn, job_id, worker_id, "still failing")  # reschedules into the future

    assert claim_next_job(conn, worker_id) is None
