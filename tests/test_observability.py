from datetime import datetime, timedelta, timezone

from durable_queue.jobs import (
    claim_next_job,
    enqueue,
    get_job,
    get_queue_stats,
    list_jobs,
    mark_failed,
    retry_job,
)


def test_get_job_returns_none_for_unknown_id(conn):
    assert get_job(conn, 999999) is None


def test_get_job_returns_full_row(conn):
    job_id = enqueue(conn, "some_task", {"a": 1})
    conn.commit()

    job = get_job(conn, job_id)

    assert job["id"] == job_id
    assert job["task"] == "some_task"
    assert job["args"] == {"a": 1}
    assert job["status"] == "pending"


def test_list_jobs_returns_all_by_default(conn):
    enqueue(conn, "task_a", {})
    enqueue(conn, "task_b", {})
    conn.commit()

    assert len(list_jobs(conn)) == 2


def test_list_jobs_filters_by_status(conn, worker_id):
    enqueue(conn, "task_a", {})
    job_id_b = enqueue(conn, "task_b", {})
    conn.commit()
    claim_next_job(conn, worker_id)  # claims the oldest (task_a) -> running

    pending_jobs = list_jobs(conn, status="pending")

    assert len(pending_jobs) == 1
    assert pending_jobs[0]["id"] == job_id_b


def test_list_jobs_respects_limit(conn):
    for _ in range(5):
        enqueue(conn, "some_task", {})
    conn.commit()

    assert len(list_jobs(conn, limit=2)) == 2


def test_retry_job_resets_a_dead_job_to_pending(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET max_attempts = 1 WHERE id = %s", (job_id,))
    conn.commit()
    claim_next_job(conn, worker_id)
    mark_failed(conn, job_id, worker_id, "boom")  # attempts exhausted -> dead

    assert retry_job(conn, job_id) is True

    job = get_job(conn, job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert job["finished_at"] is None


def test_retry_job_returns_false_for_a_non_dead_job(conn):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()

    assert retry_job(conn, job_id) is False


def test_get_queue_stats_counts_by_status(conn):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO jobs (task, status) VALUES ('t', 'pending')")
        cur.execute("INSERT INTO jobs (task, status) VALUES ('t', 'running')")
        cur.execute("INSERT INTO jobs (task, status) VALUES ('t', 'succeeded')")
        cur.execute("INSERT INTO jobs (task, status) VALUES ('t', 'succeeded')")
        cur.execute("INSERT INTO jobs (task, status) VALUES ('t', 'dead')")
    conn.commit()

    stats = get_queue_stats(conn)

    assert stats["pending"] == 1
    assert stats["running"] == 1
    assert stats["succeeded"] == 2
    assert stats["dead"] == 1


def test_get_queue_stats_oldest_pending_age_only_counts_due_jobs(conn):
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO jobs (task, status, run_at) VALUES ('t', 'pending', %s)", (past,))
        cur.execute("INSERT INTO jobs (task, status, run_at) VALUES ('t', 'pending', %s)", (future,))
    conn.commit()

    stats = get_queue_stats(conn)

    assert stats["oldest_pending_age_seconds"] is not None
    assert stats["oldest_pending_age_seconds"] >= 30


def test_get_queue_stats_oldest_pending_age_is_none_when_nothing_is_due(conn):
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO jobs (task, status, run_at) VALUES ('t', 'pending', %s)", (future,))
    conn.commit()

    stats = get_queue_stats(conn)

    assert stats["oldest_pending_age_seconds"] is None
