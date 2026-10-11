import logging
import threading
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
from psycopg.types.json import Jsonb

from durable_queue import wakeup
from durable_queue.db import RECONNECT_MAX_DELAY, RECONNECT_MIN_DELAY, ReconnectingConnection
from durable_queue.jobs import enqueue

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Clock:
    """
    A schedule that runs at a time on the clock rather than every N
    seconds. Build one with daily() or hourly().
    """
    every: str  # 'day' or 'hour'
    at: time  # for 'hour', only the minute counts
    time_zone: str


def daily(at: str, time_zone: str = "UTC") -> Clock:
    """
    Every day at `at` ("HH:MM") on time_zone's clock, so
    daily("03:00", "America/New_York") stays at 3am Eastern through
    daylight saving changes, which a 24-hour interval can't.

    On the night the clocks go forward, a time that doesn't exist (2:30
    in New York) runs an hour later on the wall clock. On the night they
    go back, a time that happens twice runs the first time only.
    """
    ZoneInfo(time_zone)  # an unknown zone fails here, not when it comes due
    return Clock("day", time.fromisoformat(at), time_zone)


def hourly(minute: int = 0) -> Clock:
    """Every hour at `minute` past, on UTC's clock."""
    if not 0 <= minute < 60:
        raise ValueError(f"minute must be 0-59, not {minute}")
    return Clock("hour", time(0, minute), "UTC")


def _on(clock: Clock, day: date) -> datetime:
    """A daily clock's time on `day`, as a UTC instant."""
    # Converted to UTC before anything compares it: two datetimes in the
    # same zone compare by wall time and ignore fold, which is wrong in
    # the hour the clocks go back.
    return datetime.combine(day, clock.at, tzinfo=ZoneInfo(clock.time_zone)).astimezone(timezone.utc)


def _clock_at_or_before(clock: Clock, instant: datetime) -> datetime:
    if clock.every == "hour":
        slot = instant.astimezone(timezone.utc).replace(minute=clock.at.minute, second=0, microsecond=0)
        return slot if slot <= instant else slot - timedelta(hours=1)
    day = instant.astimezone(ZoneInfo(clock.time_zone)).date()
    while _on(clock, day) > instant:
        day -= timedelta(days=1)
    return _on(clock, day)


def _clock_after(clock: Clock, instant: datetime) -> datetime:
    if clock.every == "hour":
        return _clock_at_or_before(clock, instant) + timedelta(hours=1)
    day = instant.astimezone(ZoneInfo(clock.time_zone)).date()
    while _on(clock, day) <= instant:
        day += timedelta(days=1)
    return _on(clock, day)


def register_schedule(
        conn: psycopg.Connection,
        name: str,
        task: str,
        args: dict,
        when: "int | Clock",
        *,
        first_run_at: datetime | None = None,
        max_lateness_seconds: int | None = None) -> None:
    """
    Create or update a recurring schedule. `when` is an interval in
    seconds, or a clock time from daily() or hourly().

    Safe to call on every app startup: re-registering an existing name
    updates its definition but leaves next_run_at untouched, so a
    redeploy doesn't reset - or skip - where the schedule sits in its
    cycle. The exception is a clock schedule moved to a different time,
    which starts over from the new one. Keeping the old next_run_at
    would run it at both times on the day of the change.

    first_run_at anchors an interval schedule's cycle. A clock
    schedule's cycle is the clock.

    max_lateness_seconds skips a run that would start more than that
    long after its time - after an outage, say - instead of running it
    late. A push notification at 2am because the 11am run was missed is
    worse than none.
    """
    if isinstance(when, Clock):
        if first_run_at is not None:
            raise ValueError("first_run_at only applies to an interval schedule")
        with conn.cursor() as cur:
            cur.execute("SELECT now() AS now")
            first_run_at = _clock_after(when, cur.fetchone()["now"])
        interval_seconds, every, at_time, time_zone = None, when.every, when.at, when.time_zone
    else:
        interval_seconds, every, at_time, time_zone = when, None, None, None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO schedules (
                name, task, args, interval_seconds, every, at_time, time_zone,
                max_lateness_seconds, next_run_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()))
            ON CONFLICT (name) DO UPDATE
            SET task = EXCLUDED.task, args = EXCLUDED.args,
                interval_seconds = EXCLUDED.interval_seconds,
                every = EXCLUDED.every, at_time = EXCLUDED.at_time,
                time_zone = EXCLUDED.time_zone,
                max_lateness_seconds = EXCLUDED.max_lateness_seconds,
                next_run_at = CASE
                    WHEN (schedules.every, schedules.at_time, schedules.time_zone)
                         IS DISTINCT FROM (EXCLUDED.every, EXCLUDED.at_time, EXCLUDED.time_zone)
                    THEN EXCLUDED.next_run_at
                    ELSE schedules.next_run_at
                END
            """,
            (
                name, task, Jsonb(args), interval_seconds, every, at_time, time_zone,
                max_lateness_seconds, first_run_at,
            ),
        )
    conn.commit()
    wakeup.wake()  # an idle scheduler here may be asleep until a later time


def _slot_to_run(row: dict) -> datetime:
    """The most recent time a due schedule should have run, at or before now."""
    if row["every"] is None:
        period = timedelta(seconds=row["interval_seconds"])
        return row["next_run_at"] + ((row["now"] - row["next_run_at"]) // period) * period
    return _clock_at_or_before(Clock(row["every"], row["at_time"], row["time_zone"]), row["now"])


def _slot_after(row: dict, slot: datetime) -> datetime:
    if row["every"] is None:
        return slot + timedelta(seconds=row["interval_seconds"])
    return _clock_after(Clock(row["every"], row["at_time"], row["time_zone"]), slot)


def run_due_schedules(conn: psycopg.Connection) -> int:
    """
    Enqueue a job for every schedule whose next_run_at has passed, then
    move next_run_at to its next time after now. Returns the count fired.

    A schedule that missed several runs - the scheduler was down - runs
    once, for the most recent one, rather than once per missed run back
    to back. With max_lateness_seconds set it doesn't run at all if even
    that one is too far in the past.

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

    The idempotency key names the slot being run, which follows from the
    schedule's definition and its stored next_run_at, not from the
    exact moment this happens to execute: any pass within the same
    period computes the same slot, so it can't drift the way a freshly
    computed "truncate now() to the hour" could across a restart. It is
    the backstop for what the locks can't see: if next_run_at is ever
    wound back to a slot that already fired (a restore from backup, a
    hand edit), firing it again recomputes the identical key, and
    enqueue() returns the existing job instead of creating a second one.
    Normalised to UTC, so the key doesn't depend on the session's
    TimeZone either.
    """
    fired = 0
    # Explicit, because the locks are only worth anything if they are
    # held until the commit - on an autocommit connection the SELECT
    # would release them the moment it finished.
    with conn.transaction():
        with conn.cursor() as cur:
            # The database's clock, not this host's, so schedulers on
            # different machines agree on what is due and when.
            cur.execute(
                """
                SELECT name, task, args, interval_seconds, every, at_time, time_zone,
                       max_lateness_seconds, next_run_at, now() AS now
                FROM schedules
                WHERE next_run_at <= now()
                FOR UPDATE SKIP LOCKED
                """
            )
            due = cur.fetchall()

        for row in due:
            slot = _slot_to_run(row)
            lateness = (row["now"] - slot).total_seconds()
            if row["max_lateness_seconds"] is not None and lateness > row["max_lateness_seconds"]:
                logger.warning(
                    "scheduler: skipped %s's run for %s, %.0fs late",
                    row["name"], slot.isoformat(), lateness,
                )
            else:
                idempotency_key = f"sched:{row['name']}:{slot.astimezone(timezone.utc).isoformat()}"
                enqueue(conn, row["task"], row["args"], idempotency_key=idempotency_key)
                fired += 1

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE schedules SET next_run_at = %s WHERE name = %s",
                    (_slot_after(row, slot), row["name"]),
                )

    # If the caller already had a transaction open, transaction() above
    # was only a savepoint inside it, so this is still what commits.
    conn.commit()
    if fired:
        wakeup.wake()  # for an idle worker in this process, which can't hear the NOTIFY
    return fired


def seconds_until_next_schedule(conn: psycopg.Connection) -> float | None:
    """How long until the next schedule is due. None if there are none."""
    with conn.cursor() as cur:
        cur.execute("SELECT extract(epoch FROM min(next_run_at) - now()) AS seconds FROM schedules")
        seconds = cur.fetchone()["seconds"]
    conn.commit()
    return None if seconds is None else float(seconds)


def run_scheduler(
        poll_interval: float = 1.0,
        dsn: str | None = None,
        *,
        idle: bool = False,
        stop: threading.Event | None = None) -> None:
    """
    A connection that drops is reopened on its next use, after a
    backoff. A pass it interrupts is safe to repeat: it never committed,
    so its enqueues rolled back with it and its locks were released.

    idle=True sleeps until the next schedule is due instead of polling,
    without a query in between, and closes its connection first if
    that's more than a minute off. register_schedule() in this process
    wakes it to look again; a schedule registered from another process
    isn't seen until its next wake. When it fires schedules it wakes
    this process's idle worker to run them. See run_worker's idle.

    stop, once set, ends it. Call wakeup.wake() after setting it, or an
    idle scheduler sleeps on until its next schedule.
    """
    conn = ReconnectingConnection(dsn)
    conn.get()  # fail at startup on a bad DSN, not retry it forever
    retry_delay = RECONNECT_MIN_DELAY
    while stop is None or not stop.is_set():
        seen = wakeup.current()
        try:
            run_due_schedules(conn.get())
            seconds = seconds_until_next_schedule(conn.get()) if idle else None
        except psycopg.OperationalError:
            logger.warning(
                "scheduler: lost its database connection, retrying in %.0fs",
                retry_delay, exc_info=True,
            )
            conn.close()
            wakeup.pause(retry_delay, stop)
            retry_delay = min(retry_delay * 2, RECONNECT_MAX_DELAY)
            continue
        retry_delay = RECONNECT_MIN_DELAY

        if not idle:
            wakeup.pause(poll_interval, stop)
            continue
        timeout = None if seconds is None else max(seconds, wakeup.MIN_SLEEP)
        if timeout is None or timeout > wakeup.CLOSE_CONNECTIONS_AFTER:
            conn.close()
        wakeup.wait(seen, timeout)
    conn.close()


if __name__ == "__main__":
    run_scheduler()
