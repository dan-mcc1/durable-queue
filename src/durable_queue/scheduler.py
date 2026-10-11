import logging
from datetime import datetime, timedelta
from time import sleep

import psycopg
from psycopg.types.json import Jsonb

from durable_queue.db import RECONNECT_MAX_DELAY, RECONNECT_MIN_DELAY, ReconnectingConnection
from durable_queue.jobs import enqueue

logger = logging.getLogger(__name__)


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


def run_due_schedules(conn: psycopg.Connection) -> int:
    """
    Enqueue a job for every schedule whose next_run_at has passed, then
    advance next_run_at by its interval. Returns the count fired.

    Safe to run from any number of schedulers at once, so there is no
    leader. Due schedules are locked FOR UPDATE SKIP LOCKED - the same
    move as claiming a job - and held until the enqueues and the
    advanced next_run_at commit together. A concurrent scheduler skips
    whatever this one holds, and by the time the locks release those
    schedules are no longer due. A scheduler that crashes mid-run rolls
    back and releases its locks, so the next one simply fires them.

    This replaced pg_try_advisory_lock leader election, and is better
    in two ways besides being less code. A leader that hung while
    keeping its connection open held the lock forever, and nothing
    fired; here a hung scheduler only stalls the schedules it has
    locked. And a session-scoped advisory lock needs a stable session,
    which a transaction-pooling PgBouncer doesn't provide; row locks
    live inside the transaction and work through it.

    The idempotency key is built from the schedule's own stored
    next_run_at, not from now(). That value is persisted and doesn't
    change until this function advances it, so it can't drift the way a
    freshly computed "truncate now() to the hour" could across a
    restart. It is the backstop for what the locks can't see: if
    next_run_at is ever wound back to a slot that already fired (a
    restore from backup, a hand edit), firing it again recomputes the
    identical key, and enqueue() returns the existing job instead of
    creating a second one.
    """
    # Explicit, because the locks are only worth anything if they are
    # held until the commit - on an autocommit connection the SELECT
    # would release them the moment it finished.
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, task, args, interval_seconds, next_run_at FROM schedules
                WHERE next_run_at <= now()
                FOR UPDATE SKIP LOCKED
                """
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

    # If the caller already had a transaction open, transaction() above
    # was only a savepoint inside it, so this is still what commits.
    conn.commit()
    return len(due)


def run_scheduler(poll_interval: float = 1.0, dsn: str | None = None) -> None:
    """
    A connection that drops is reopened on its next use, after a
    backoff. A pass it interrupts is safe to repeat: it never committed,
    so its enqueues rolled back with it and its locks were released.
    """
    conn = ReconnectingConnection(dsn)
    conn.get()  # fail at startup on a bad DSN, not retry it forever
    retry_delay = RECONNECT_MIN_DELAY
    while True:
        try:
            run_due_schedules(conn.get())
        except psycopg.OperationalError:
            logger.warning(
                "scheduler: lost its database connection, retrying in %.0fs",
                retry_delay, exc_info=True,
            )
            conn.close()
            sleep(retry_delay)
            retry_delay = min(retry_delay * 2, RECONNECT_MAX_DELAY)
            continue
        retry_delay = RECONNECT_MIN_DELAY
        sleep(poll_interval)


if __name__ == "__main__":
    run_scheduler()
