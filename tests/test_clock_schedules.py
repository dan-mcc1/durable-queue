"""
Schedules on the clock - 3am in New York, every hour on the hour - and
what happens when a scheduler has missed runs: one run for the most
recent, not one per missed run back to back.
"""
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pytest

from durable_queue.scheduler import (
    _clock_after,
    _clock_at_or_before,
    daily,
    hourly,
    register_schedule,
    run_due_schedules,
)

NEW_YORK = ZoneInfo("America/New_York")


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _schedule(conn, name: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT *, now() AS now FROM schedules WHERE name = %s", (name,))
        return cur.fetchone()


def _keys(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT idempotency_key FROM jobs ORDER BY id")
        return [row["idempotency_key"] for row in cur.fetchall()]


def test_a_daily_time_stays_put_on_the_local_clock_across_dst():
    three_am = daily("03:00", "America/New_York")
    # 3am EDT is 07:00 UTC; after the clocks go back on 1 Nov, 3am EST is 08:00.
    assert _clock_after(three_am, _utc(2026, 10, 31, 12)) == _utc(2026, 11, 1, 8)
    assert _clock_after(three_am, _utc(2026, 10, 30, 12)) == _utc(2026, 10, 31, 7)
    assert _clock_at_or_before(three_am, _utc(2026, 11, 1, 7, 59)) == _utc(2026, 10, 31, 7)


def test_a_time_the_clocks_skip_runs_an_hour_later():
    # 8 Mar 2026: New York jumps from 02:00 EST to 03:00 EDT, so 02:30 never happens.
    assert _clock_after(daily("02:30", "America/New_York"), _utc(2026, 3, 8, 5)) == _utc(2026, 3, 8, 7, 30)


def test_a_time_that_happens_twice_runs_once():
    one_thirty = daily("01:30", "America/New_York")
    first = _clock_after(one_thirty, _utc(2026, 11, 1, 4))  # midnight EDT
    assert first == _utc(2026, 11, 1, 5, 30)  # 01:30 EDT
    # Not 06:30 UTC the same night - that's 01:30 again, now EST.
    assert _clock_after(one_thirty, first) == _utc(2026, 11, 2, 6, 30)


def test_hourly_runs_at_its_minute_past_on_utc():
    quarter_past = hourly(15)
    assert _clock_after(quarter_past, _utc(2026, 10, 10, 10, 20)) == _utc(2026, 10, 10, 11, 15)
    assert _clock_at_or_before(quarter_past, _utc(2026, 10, 10, 10, 20)) == _utc(2026, 10, 10, 10, 15)
    assert _clock_after(quarter_past, _utc(2026, 10, 10, 10, 15)) == _utc(2026, 10, 10, 11, 15)


def test_bad_definitions_fail_at_registration_not_when_due(conn):
    with pytest.raises(ZoneInfoNotFoundError):
        daily("03:00", "America/Nowhere")
    with pytest.raises(ValueError):
        hourly(60)
    with pytest.raises(ValueError):
        register_schedule(
            conn, "s", "some_task", {}, hourly(), first_run_at=datetime.now(timezone.utc),
        )


def test_a_daily_schedule_is_first_due_at_its_next_local_time(conn):
    register_schedule(conn, "stripe", "some_task", {}, daily("04:00", "America/New_York"))

    row = _schedule(conn, "stripe")
    assert row["next_run_at"].astimezone(NEW_YORK).time() == time(4)
    assert row["now"] < row["next_run_at"] <= row["now"] + timedelta(days=1)


def test_missed_interval_runs_fire_once_for_the_latest(conn):
    """
    Five and a half hours down with an hourly schedule: it missed five
    runs, runs once for the latest (half an hour ago), and carries on
    from there.
    """
    anchor = datetime.now(timezone.utc) - timedelta(hours=5, minutes=30)
    register_schedule(conn, "hourly", "some_task", {}, 3600, first_run_at=anchor)

    assert run_due_schedules(conn) == 1
    assert run_due_schedules(conn) == 0

    latest = anchor + timedelta(hours=5)
    assert _keys(conn) == [f"sched:hourly:{latest.isoformat()}"]
    assert _schedule(conn, "hourly")["next_run_at"] == latest + timedelta(hours=1)


def test_missed_daily_runs_fire_once_for_the_latest(conn):
    register_schedule(conn, "refresh", "some_task", {}, daily("03:00", "America/New_York"))
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE schedules SET next_run_at = next_run_at - interval '3 days' WHERE name = 'refresh'"
        )
    conn.commit()

    assert run_due_schedules(conn) == 1

    row = _schedule(conn, "refresh")
    ran_for = datetime.fromisoformat(_keys(conn)[0].removeprefix("sched:refresh:"))
    assert ran_for.astimezone(NEW_YORK).time() == time(3)
    assert row["now"] - timedelta(days=1) < ran_for <= row["now"]
    assert row["next_run_at"] == _clock_after(daily("03:00", "America/New_York"), ran_for)


def test_a_run_later_than_max_lateness_is_skipped(conn):
    # Latest slot is 30 minutes ago: too late for 10 minutes, fine for an hour.
    anchor = datetime.now(timezone.utc) - timedelta(minutes=90)
    register_schedule(conn, "strict", "some_task", {}, 3600, first_run_at=anchor,
                      max_lateness_seconds=600)
    register_schedule(conn, "lenient", "some_task", {}, 3600, first_run_at=anchor,
                      max_lateness_seconds=3600)

    assert run_due_schedules(conn) == 1

    assert _keys(conn) == [f"sched:lenient:{(anchor + timedelta(hours=1)).isoformat()}"]
    # Skipped, not stuck: it moves on to its next slot all the same.
    assert _schedule(conn, "strict")["next_run_at"] == anchor + timedelta(hours=2)


def test_moving_a_clock_schedule_starts_over_from_the_new_time(conn):
    """
    Keeping next_run_at, as an unchanged re-registration does, would
    leave it due at 3am once more after being moved to 4am - two runs
    on the day of the change.
    """
    register_schedule(conn, "refresh", "some_task", {}, daily("03:00", "America/New_York"))
    register_schedule(conn, "refresh", "some_task", {}, daily("04:00", "America/New_York"))
    moved = _schedule(conn, "refresh")["next_run_at"]
    assert moved.astimezone(NEW_YORK).time() == time(4)

    register_schedule(conn, "refresh", "other_task", {"v": 2}, daily("04:00", "America/New_York"))
    row = _schedule(conn, "refresh")
    assert row["task"] == "other_task"
    assert row["next_run_at"] == moved
