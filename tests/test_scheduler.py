from datetime import datetime, timedelta, timezone

from durable_queue.db import get_connection
from durable_queue.scheduler import register_schedule, run_due_schedules, try_acquire_leadership


def test_try_acquire_leadership_succeeds_when_uncontended():
    conn = get_connection()
    try:
        assert try_acquire_leadership(conn) is True
    finally:
        conn.close()


def test_try_acquire_leadership_fails_when_already_held():
    holder = get_connection()
    challenger = get_connection()
    try:
        assert try_acquire_leadership(holder) is True
        assert try_acquire_leadership(challenger) is False
    finally:
        holder.close()
        challenger.close()


def test_leadership_is_released_when_connection_closes():
    holder = get_connection()
    challenger = get_connection()
    try:
        assert try_acquire_leadership(holder) is True
        assert try_acquire_leadership(challenger) is False

        holder.close()

        assert try_acquire_leadership(challenger) is True
    finally:
        challenger.close()


def test_register_schedule_creates_new_schedule(conn):
    past = datetime.now(timezone.utc) - timedelta(hours=1)

    register_schedule(conn, "hourly-digest", "send_digest", {"x": 1}, 3600, first_run_at=past)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT task, args, interval_seconds, next_run_at FROM schedules WHERE name = %s",
            ("hourly-digest",),
        )
        row = cur.fetchone()

    assert row["task"] == "send_digest"
    assert row["args"] == {"x": 1}
    assert row["interval_seconds"] == 3600
    assert row["next_run_at"] == past


def test_register_schedule_updates_definition_without_resetting_next_run_at(conn):
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    register_schedule(conn, "hourly-digest", "send_digest", {}, 3600, first_run_at=past)

    register_schedule(conn, "hourly-digest", "send_digest_v2", {"y": 2}, 1800)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT task, args, interval_seconds, next_run_at FROM schedules WHERE name = %s",
            ("hourly-digest",),
        )
        row = cur.fetchone()

    assert row["task"] == "send_digest_v2"
    assert row["args"] == {"y": 2}
    assert row["interval_seconds"] == 1800
    assert row["next_run_at"] == past


def test_run_due_schedules_enqueues_and_advances_next_run_at(conn):
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    register_schedule(conn, "hourly-digest", "some_task", {"z": 3}, 3600, first_run_at=past)

    fired = run_due_schedules(conn)

    assert fired == 1
    with conn.cursor() as cur:
        cur.execute("SELECT args FROM jobs WHERE task = %s", ("some_task",))
        job_row = cur.fetchone()
    assert job_row["args"] == {"z": 3}

    with conn.cursor() as cur:
        cur.execute("SELECT next_run_at FROM schedules WHERE name = %s", ("hourly-digest",))
        new_next_run_at = cur.fetchone()["next_run_at"]
    assert new_next_run_at == past + timedelta(seconds=3600)


def test_run_due_schedules_skips_a_schedule_not_yet_due(conn):
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    register_schedule(conn, "future-sched", "some_task", {}, 3600, first_run_at=future)

    assert run_due_schedules(conn) == 0


def test_run_due_schedules_does_not_double_enqueue_if_rerun_before_advancing(conn):
    """
    Simulates a crash between enqueueing and advancing next_run_at: run
    it once, then reset next_run_at back to what it was - as if that
    UPDATE never happened - and run it again. The idempotency key is
    derived from next_run_at itself, so recomputing the same
    next_run_at must produce the same key. This is the backstop that
    keeps a scheduler crash from double-firing a job even though leader
    election alone doesn't prevent it.
    """
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    register_schedule(conn, "test-sched", "some_task", {}, 3600, first_run_at=past)

    run_due_schedules(conn)

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE schedules SET next_run_at = %s WHERE name = %s",
            (past, "test-sched"),
        )
    conn.commit()

    run_due_schedules(conn)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM jobs WHERE task = %s", ("some_task",))
        assert cur.fetchone()["n"] == 1
