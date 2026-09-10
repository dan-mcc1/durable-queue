"""
Deleting completed jobs. Needed because HOT updates are impossible on
this table (status sits in both partial index predicates), so a busy
queue bloats its heap continuously and only staying small keeps claims
and vacuum cheap.
"""
from datetime import datetime, timedelta, timezone

from durable_queue.jobs import claim_jobs, delete_completed_jobs, enqueue, get_job


def _insert(conn, *, status: str, finished_minutes_ago: float | None) -> int:
    finished_at = (
        None if finished_minutes_ago is None
        else datetime.now(timezone.utc) - timedelta(minutes=finished_minutes_ago)
    )
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO jobs (task, status, finished_at) VALUES ('t', %s, %s) RETURNING id",
            (status, finished_at),
        )
        job_id = cur.fetchone()["id"]
    conn.commit()
    return job_id


def test_deletes_completed_jobs_past_the_retention_window(conn):
    old_succeeded = _insert(conn, status="succeeded", finished_minutes_ago=120)
    old_dead = _insert(conn, status="dead", finished_minutes_ago=120)

    assert delete_completed_jobs(conn, older_than_seconds=3600) == 2
    assert get_job(conn, old_succeeded) is None
    assert get_job(conn, old_dead) is None


def test_keeps_completed_jobs_inside_the_retention_window(conn):
    recent = _insert(conn, status="succeeded", finished_minutes_ago=5)

    assert delete_completed_jobs(conn, older_than_seconds=3600) == 0
    assert get_job(conn, recent) is not None


def test_never_deletes_unfinished_jobs(conn, worker_id):
    pending_id = enqueue(conn, "some_task", {})
    conn.commit()
    running_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_jobs(conn, worker_id, batch_size=10)

    # Old enough to qualify on age alone, but neither has finished.
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET finished_at = now() - interval '1 day'")
    conn.commit()

    assert delete_completed_jobs(conn, older_than_seconds=60) == 0
    assert get_job(conn, pending_id) is not None
    assert get_job(conn, running_id) is not None


def test_respects_the_batch_limit(conn):
    for _ in range(5):
        _insert(conn, status="succeeded", finished_minutes_ago=120)

    assert delete_completed_jobs(conn, older_than_seconds=3600, batch=2) == 2
    assert delete_completed_jobs(conn, older_than_seconds=3600, batch=2) == 2
    assert delete_completed_jobs(conn, older_than_seconds=3600, batch=2) == 1
    assert delete_completed_jobs(conn, older_than_seconds=3600, batch=2) == 0
