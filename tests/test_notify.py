"""
LISTEN/NOTIFY: an enqueue wakes an idle worker immediately instead of
it waiting out a poll interval, and does so transactionally.
"""
from time import monotonic

from durable_queue.db import get_connection
from durable_queue.jobs import enqueue
from durable_queue.worker import (
    listen_for_jobs,
    relax_bookkeeping_durability,
    wait_for_job,
)


def test_relax_bookkeeping_durability_applies_to_that_connection_only(conn):
    relaxed = get_connection()
    try:
        relax_bookkeeping_durability(relaxed)

        assert relaxed.execute("SHOW synchronous_commit").fetchone()["synchronous_commit"] == "off"
        # Other connections - notably whichever one the application
        # enqueues through - are untouched and stay durable.
        assert conn.execute("SHOW synchronous_commit").fetchone()["synchronous_commit"] == "on"
    finally:
        relaxed.close()


def test_wait_for_job_times_out_when_nothing_is_enqueued(conn):
    listener = get_connection()
    try:
        listen_for_jobs(listener)

        started = monotonic()
        woke = wait_for_job(listener, 0.3)

        assert woke is False
        assert monotonic() - started >= 0.3
    finally:
        listener.close()


def test_enqueue_wakes_a_listening_worker(conn):
    listener = get_connection()
    try:
        listen_for_jobs(listener)

        enqueue(conn, "some_task", {})
        conn.commit()

        started = monotonic()
        assert wait_for_job(listener, 5.0) is True
        # Woken by the notification, not by the timeout elapsing.
        assert monotonic() - started < 1.0
    finally:
        listener.close()


def test_a_rolled_back_enqueue_wakes_nobody(conn):
    """
    Postgres only delivers NOTIFY on commit. That's exactly the
    behaviour the whole design wants: work that was never really
    enqueued must not wake a worker to look for it.
    """
    listener = get_connection()
    try:
        listen_for_jobs(listener)

        enqueue(conn, "some_task", {})
        conn.rollback()

        assert wait_for_job(listener, 0.3) is False
    finally:
        listener.close()
