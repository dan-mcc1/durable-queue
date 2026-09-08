"""
Transactional task execution: a task that declares a `conn` parameter
runs inside the worker's transaction, so its own writes and the job's
completion commit together - or not at all.
"""
from durable_queue.jobs import enqueue, get_job
from durable_queue.registry import get_registered_task, task
from durable_queue.worker import process_one


@task
def txn_writes_a_row(conn, effect_key: str) -> None:
    # No commit and no close: the connection belongs to the worker, and
    # the worker's transaction is what makes this durable.
    with conn.cursor() as cur:
        cur.execute("INSERT INTO chaos_observations (effect_key) VALUES (%s)", (effect_key,))


@task
def txn_writes_then_raises(conn, effect_key: str) -> None:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO chaos_observations (effect_key) VALUES (%s)", (effect_key,))
    raise RuntimeError("boom")


@task
def plain_task_without_connection(message: str) -> None:
    pass


def _observation_count(conn, effect_key: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM chaos_observations WHERE effect_key = %s",
            (effect_key,),
        )
        return cur.fetchone()["n"]


def test_registry_detects_which_tasks_want_a_connection():
    assert get_registered_task("txn_writes_a_row").wants_connection is True
    assert get_registered_task("plain_task_without_connection").wants_connection is False


def test_transactional_task_commits_its_writes_with_the_job(conn, worker_id):
    job_id = enqueue(conn, "txn_writes_a_row", {"effect_key": "txn-ok"})
    conn.commit()

    assert process_one(conn, worker_id) is True

    assert get_job(conn, job_id)["status"] == "succeeded"
    assert _observation_count(conn, "txn-ok") == 1


def test_failing_transactional_task_rolls_its_writes_back(conn, worker_id):
    """
    The write and the completion share a transaction, so a task that
    raises leaves nothing behind - no half-applied side effect to
    reconcile on retry.
    """
    job_id = enqueue(conn, "txn_writes_then_raises", {"effect_key": "txn-rollback"})
    conn.commit()

    process_one(conn, worker_id)

    assert _observation_count(conn, "txn-rollback") == 0

    job = get_job(conn, job_id)
    assert job["status"] == "pending"  # attempts remain, so it's rescheduled
    assert job["attempts"] == 1
    assert "boom" in job["last_error"]


def test_non_transactional_tasks_still_run_normally(conn, worker_id):
    job_id = enqueue(conn, "plain_task_without_connection", {"message": "hi"})
    conn.commit()

    assert process_one(conn, worker_id) is True
    assert get_job(conn, job_id)["status"] == "succeeded"
