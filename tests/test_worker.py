from durable_queue.jobs import enqueue
from durable_queue.registry import PermanentError, task
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


@task
def rejects_its_input() -> None:
    raise PermanentError("record 42 no longer exists")


@task(max_attempts=2)
def flaky_with_a_short_fuse() -> None:
    raise RuntimeError("upstream timed out")


@task(max_execution_seconds=5)
def bounded_runtime() -> None:
    pass


def _claim_and_run_again(conn, worker_id, job_id) -> dict:
    """Make a rescheduled job due now and run it, skipping its backoff."""
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET run_at = now() WHERE id = %s", (job_id,))
    conn.commit()
    process_one(conn, worker_id)
    with conn.cursor() as cur:
        cur.execute("SELECT status, attempts, max_attempts, last_error FROM jobs WHERE id = %s", (job_id,))
        return cur.fetchone()


def test_permanent_error_dead_letters_without_retrying(conn, worker_id):
    job_id = enqueue(conn, "rejects_its_input", {})
    conn.commit()

    process_one(conn, worker_id)

    with conn.cursor() as cur:
        cur.execute("SELECT status, attempts, last_error FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == 1  # it still counts as the attempt it was
    assert "no longer exists" in row["last_error"]


def test_task_max_attempts_overrides_the_row_default(conn, worker_id):
    job_id = enqueue(conn, "flaky_with_a_short_fuse", {})
    conn.commit()

    process_one(conn, worker_id)
    row = _claim_and_run_again(conn, worker_id, job_id)
    assert row["status"] == "dead"
    assert row["attempts"] == 2
    # Written to the row, so `durable-queue show` reports the limit
    # that was actually applied rather than the table default of 5.
    assert row["max_attempts"] == 2


class _RecordingHeartbeat:
    def __init__(self) -> None:
        self.deadlines: dict[int, float | None] = {}

    def hold(self, job_ids, max_execution_seconds=None) -> None:
        for job_id in job_ids:
            self.deadlines[job_id] = max_execution_seconds

    def release(self, job_ids) -> None:
        pass


def test_task_max_execution_seconds_overrides_the_worker_default(conn, worker_id):
    bounded_id = enqueue(conn, "bounded_runtime", {})
    default_id = enqueue(conn, "succeed_loudly", {"message": "hi"})
    conn.commit()
    heartbeat = _RecordingHeartbeat()

    process_one(conn, worker_id, max_execution_seconds=300, heartbeat=heartbeat)
    process_one(conn, worker_id, max_execution_seconds=300, heartbeat=heartbeat)

    assert heartbeat.deadlines == {bounded_id: 5, default_id: 300}
