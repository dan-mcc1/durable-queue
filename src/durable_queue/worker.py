import os
import socket
import threading
import uuid
from time import monotonic, sleep

import psycopg

from durable_queue.db import get_connection
from durable_queue.jobs import (
    DEFAULT_LEASE_SECONDS,
    claim_next_job,
    extend_lease,
    mark_failed,
    mark_succeeded,
    reap_expired_jobs,
)
from durable_queue.registry import get_task


DEFAULT_MAX_EXECUTION_SECONDS = 300


def generate_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _heartbeat_loop(
        conn: psycopg.Connection,
        job_id: int,
        worker_id: str,
        lease_seconds: int,
        interval: float,
        stop_event: threading.Event,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS) -> None:
    # stop_event.wait(interval) returns True as soon as the event is set,
    # False if it timed out — so this extends the lease on every timeout
    # and stops cleanly the moment the caller signals it's done.
    #
    # The deadline is what stops a hung task from being propped up
    # forever: an unbounded heartbeat defeats the reaper entirely, since
    # a task blocked on a network call with no timeout would keep its
    # lease alive indefinitely and never be recovered. Past the ceiling
    # we stop extending and let the lease lapse, so the job goes back in
    # the queue even though this process is still stuck on it.
    deadline = monotonic() + max_execution_seconds
    while not stop_event.wait(interval):
        if monotonic() >= deadline:
            return
        extend_lease(conn, job_id, worker_id, lease_seconds)


def process_one(
        conn: psycopg.Connection,
        worker_id: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS) -> bool:
    """
    Reap any expired leases, then claim and run a single job, if one is
    available.

    Returns True if a job was claimed (regardless of success/failure),
    False if the queue was empty.
    """
    reap_expired_jobs(conn)

    job = claim_next_job(conn, worker_id, lease_seconds)
    if job is None:
        return False

    # The heartbeat runs on its own connection in a background thread,
    # since the task call below blocks the main thread for as long as
    # the task runs, and a psycopg connection isn't safe to use from
    # more than one thread at a time.
    heartbeat_conn = get_connection()
    stop_heartbeat = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(
            heartbeat_conn,
            job["id"],
            worker_id,
            lease_seconds,
            lease_seconds / 3,
            stop_heartbeat,
            max_execution_seconds,
        ),
        daemon=True,
    )
    heartbeat_thread.start()

    try:
        fn = get_task(job["task"])
        fn(**job["args"])
    except Exception as exc:
        mark_failed(conn, job["id"], worker_id, str(exc))
    else:
        mark_succeeded(conn, job["id"], worker_id)
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join()
        heartbeat_conn.close()

    return True


def run_worker(
        poll_interval: float = 1.0,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS) -> None:
    conn = get_connection()
    worker_id = generate_worker_id()
    while True:
        if not process_one(conn, worker_id, lease_seconds, max_execution_seconds):
            sleep(poll_interval)


if __name__ == "__main__":
    run_worker()
