from datetime import datetime, timedelta, timezone

import pytest

from durable_queue import scheduler
from durable_queue.db import get_connection
from durable_queue.jobs import enqueue
from durable_queue.scheduler import register_schedule, run_due_schedules


def test_concurrent_schedulers_fire_a_due_schedule_once(conn, monkeypatch):
    """
    No leader election, so two schedulers can genuinely overlap. The
    second runs in the middle of the first's pass - after it locked the
    due schedule, before it committed - which is the worst moment, and
    must neither fire the schedule again nor block waiting for it.
    """
    past = datetime.now(timezone.utc) - timedelta(minutes=90)
    register_schedule(conn, "hourly-digest", "some_task", {}, 3600, first_run_at=past)

    rival_conn = get_connection()
    rival_fired = []

    def enqueue_with_a_rival_mid_pass(*args, **kwargs):
        if not rival_fired:  # once - the rival's own pass comes through here too
            rival_fired.append(None)
            rival_fired[0] = run_due_schedules(rival_conn)
        return enqueue(*args, **kwargs)

    monkeypatch.setattr(scheduler, "enqueue", enqueue_with_a_rival_mid_pass)
    try:
        assert run_due_schedules(conn) == 1
    finally:
        rival_conn.close()

    assert rival_fired == [0]
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM jobs WHERE task = %s", ("some_task",))
        assert cur.fetchone()["n"] == 1
        cur.execute("SELECT next_run_at FROM schedules WHERE name = %s", ("hourly-digest",))
        # It ran once, for its latest slot (past + 1h), so next is the one after.
        assert cur.fetchone()["next_run_at"] == past + timedelta(hours=2)


def test_a_scheduler_that_dies_mid_pass_leaves_the_schedule_for_the_next(conn, monkeypatch):
    """
    What leader election relied on the advisory lock's auto-release
    for: a dead scheduler mustn't strand anything. Its session is
    killed server-side while it holds the due schedule's lock, so
    Postgres rolls the pass back and drops the lock, and the next
    scheduler fires the schedule.
    """
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    register_schedule(conn, "hourly-digest", "some_task", {}, 3600, first_run_at=past)

    doomed_conn = get_connection()

    def killed_mid_pass(*args, **kwargs):
        with conn.cursor() as cur:
            # The timeout makes this wait until the backend has exited.
            cur.execute("SELECT pg_terminate_backend(%s, 5000)", (doomed_conn.info.backend_pid,))
        conn.commit()
        raise RuntimeError("scheduler process killed")

    monkeypatch.setattr(scheduler, "enqueue", killed_mid_pass)
    with pytest.raises(Exception):
        run_due_schedules(doomed_conn)
    monkeypatch.undo()
    doomed_conn.close()

    assert run_due_schedules(conn) == 1


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
    past = datetime.now(timezone.utc) - timedelta(minutes=90)
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
    assert new_next_run_at == past + timedelta(hours=2)


def test_run_due_schedules_skips_a_schedule_not_yet_due(conn):
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    register_schedule(conn, "future-sched", "some_task", {}, 3600, first_run_at=future)

    assert run_due_schedules(conn) == 0


def test_run_due_schedules_does_not_double_enqueue_if_next_run_at_is_rewound(conn):
    """
    Run it once, then wind next_run_at back to the slot that already
    fired - a restore from backup, say - and run it again. Row locks
    can't help here: nothing is concurrent, the schedule simply looks
    due again. The idempotency key names the slot, which follows from
    next_run_at and the interval, so the same slot recomputes the same
    key and enqueue() dedupes it. This is the backstop behind the locks.
    """
    past = datetime.now(timezone.utc) - timedelta(minutes=90)
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
