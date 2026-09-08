from datetime import datetime, timedelta
from time import sleep

import psycopg
from psycopg.types.json import Jsonb

from durable_queue.db import get_connection
from durable_queue.jobs import enqueue

# An arbitrary fixed key identifying "the durable-queue scheduler" as a
# single named lock. Any stable int64 works here; this number carries no
# other meaning.
SCHEDULER_LOCK_KEY = 727274


def register_schedule(
        conn: psycopg.Connection,
        name: str,
        task: str,
        args: dict,
        interval_seconds: int,
        *,
        first_run_at: datetime | None = None) -> None:
    """
    Create or update a recurring schedule. Safe to call on every app
    startup: re-registering an existing name updates its task/args/
    interval but leaves next_run_at untouched, so a redeploy doesn't
    reset - or skip - where the schedule currently sits in its cycle.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO schedules (name, task, args, interval_seconds, next_run_at)
            VALUES (%s, %s, %s, %s, COALESCE(%s, now()))
            ON CONFLICT (name) DO UPDATE
            SET task = EXCLUDED.task, args = EXCLUDED.args, interval_seconds = EXCLUDED.interval_seconds
            """,
            (name, task, Jsonb(args), interval_seconds, first_run_at),
        )
    conn.commit()


def try_acquire_leadership(conn: psycopg.Connection) -> bool:
    """
    Attempt to become the scheduler leader. Non-blocking: returns
    immediately with True/False rather than waiting for the lock.

    This is a session-scoped advisory lock - Postgres releases it
    automatically if this connection dies, crashes, or closes, with no
    manual cleanup required. That's deliberate, not a limitation: it's
    what lets a second scheduler instance take over the instant the
    leader disappears, without a heartbeat or lease of its own.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (SCHEDULER_LOCK_KEY,))
        return cur.fetchone()["pg_try_advisory_lock"]


def run_due_schedules(conn: psycopg.Connection) -> int:
    """
    Enqueue a job for every schedule whose next_run_at has passed, then
    advance next_run_at by its interval. Returns the count fired.

    The idempotency key is built from the schedule's own stored
    next_run_at, not from now(). That value is persisted and doesn't
    change until this function advances it, so it can't drift the way a
    freshly computed "truncate now() to the hour" could across a
    restart. This is the backstop defense: even if this function were
    interrupted after enqueueing but before advancing next_run_at, the
    next attempt would recompute the identical key, and enqueue() would
    just return the already-existing job's id instead of creating a
    second one.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, task, args, interval_seconds, next_run_at FROM schedules WHERE next_run_at <= now()"
        )
        due = cur.fetchall()

    for row in due:
        idempotency_key = f"sched:{row['name']}:{row['next_run_at'].isoformat()}"
        enqueue(conn, row["task"], row["args"], idempotency_key=idempotency_key)

        next_run_at = row["next_run_at"] + timedelta(seconds=row["interval_seconds"])
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE schedules SET next_run_at = %s WHERE name = %s",
                (next_run_at, row["name"]),
            )

    conn.commit()
    return len(due)


def run_scheduler(poll_interval: float = 1.0) -> None:
    conn = get_connection()
    is_leader = False
    while True:
        if not is_leader:
            is_leader = try_acquire_leadership(conn)
        if is_leader:
            run_due_schedules(conn)
        sleep(poll_interval)


if __name__ == "__main__":
    run_scheduler()
