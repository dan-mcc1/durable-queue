from durable_queue.jobs import enqueue
from durable_queue.registry import task
from durable_queue.worker import process_one


@task
def succeed_loudly(message: str) -> None:
    print(message)


@task
def always_fails() -> None:
    raise RuntimeError("boom")


def test_process_one_marks_job_succeeded(conn, worker_id):
    job_id = enqueue(conn, "succeed_loudly", {"message": "hi"})
    conn.commit()

    assert process_one(conn, worker_id) is True

    with conn.cursor() as cur:
        cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
        assert cur.fetchone()["status"] == "succeeded"


def test_process_one_reschedules_a_failing_job_when_attempts_remain(conn, worker_id):
    job_id = enqueue(conn, "always_fails", {})
    conn.commit()

    process_one(conn, worker_id)

    with conn.cursor() as cur:
        cur.execute("SELECT status, attempts, last_error FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert "boom" in row["last_error"]


def test_process_one_marks_failing_job_dead_once_attempts_exhausted(conn, worker_id):
    job_id = enqueue(conn, "always_fails", {})
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET max_attempts = 1 WHERE id = %s", (job_id,))
    conn.commit()

    process_one(conn, worker_id)

    with conn.cursor() as cur:
        cur.execute("SELECT status, last_error FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    assert row["status"] == "dead"
    assert "boom" in row["last_error"]


def test_process_one_returns_false_when_queue_is_empty(conn, worker_id):
    assert process_one(conn, worker_id) is False


def test_process_one_marks_dead_when_task_is_not_registered(conn, worker_id):
    job_id = enqueue(conn, "no_such_task_registered", {})
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET max_attempts = 1 WHERE id = %s", (job_id,))
    conn.commit()

    process_one(conn, worker_id)

    with conn.cursor() as cur:
        cur.execute("SELECT status, last_error FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    assert row["status"] == "dead"
    assert "no_such_task_registered" in row["last_error"]
