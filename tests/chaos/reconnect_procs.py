"""
Worker and scheduler subprocesses for test_dropped_connections. Run as
`python -m tests.chaos.reconnect_procs <mode>` from the repo root. Not
meant to be imported.
"""
import sys

import psycopg

import tests.chaos.chaos_tasks  # noqa: F401 - import registers @tasks
from durable_queue import worker
from durable_queue.scheduler import run_scheduler
from durable_queue.worker import run_worker


def _first_borrows_fail(count: int) -> None:
    """
    The database is unreachable for the first `count` connection
    borrows: with one slot, both the first job's completion and the
    failure recorded in its place.
    """
    real_borrow = worker._borrow
    remaining = [count]

    def borrow(source):
        if remaining[0] > 0:
            remaining[0] -= 1
            raise psycopg.OperationalError("database unreachable")
        return real_borrow(source)

    worker._borrow = borrow


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "listening_worker":
        # Polling slower than any test waits, so a job that starts
        # promptly can only have been woken by a notification.
        run_worker(poll_interval=60, concurrency=2)
    elif mode == "unrecordable_first_job":
        _first_borrows_fail(2)
        run_worker(poll_interval=0.05, lease_seconds=2, concurrency=1)
    elif mode == "scheduler":
        run_scheduler(poll_interval=0.05)
    else:
        raise SystemExit(f"unknown mode {mode!r}")
