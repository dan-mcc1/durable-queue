import os
import socket
import threading
import uuid
from time import monotonic, sleep

import psycopg

from durable_queue.db import get_connection
from durable_queue.jobs import (
    DEFAULT_LEASE_SECONDS,
    JOB_NOTIFY_CHANNEL,
    claim_next_job,
    extend_lease,
    mark_failed,
    mark_succeeded,
    mark_succeeded_in_transaction,
    reap_expired_jobs,
)
from durable_queue.registry import get_registered_task


DEFAULT_MAX_EXECUTION_SECONDS = 300


class _LeaseLost(Exception):
    """
    Raised to abort a transactional task whose lease lapsed mid-run.

    Raising inside the transaction rolls the task's own writes back,
    which is the point: if another worker has already reclaimed this
    job, our work must be discarded rather than committed alongside
    theirs.
    """


def listen_for_jobs(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(f"LISTEN {JOB_NOTIFY_CHANNEL}")
    conn.commit()


def wait_for_job(conn: psycopg.Connection, timeout: float) -> bool:
    """
    Block until an enqueue notification arrives or timeout elapses.
    Returns whether a notification woke us.

    The timeout is what keeps this correct rather than merely fast:
    retries and scheduled jobs become claimable through the passage of
    time, with no NOTIFY to announce them, so polling remains the
    backstop. NOTIFY only removes the latency on the common path.
    """
    for _ in conn.notifies(timeout=timeout, stop_after=1):
        return True
    return False


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
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS,
        heartbeat_conn: psycopg.Connection | None = None) -> bool:
    """
    Reap any expired leases, then claim and run a single job, if one is
    available.

    Returns True if a job was claimed (regardless of success/failure),
    False if the queue was empty.

    Pass heartbeat_conn to reuse one connection across jobs. Opening a
    fresh one costs ~13ms against a local database - more than half the
    per-job budget at this scale - and a worker runs jobs serially, so
    there's no reason to pay it per job.
    """
    reap_expired_jobs(conn)

    job = claim_next_job(conn, worker_id, lease_seconds)
    if job is None:
        return False

    # The heartbeat runs on its own connection in a background thread,
    # since the task call below blocks the main thread for as long as
    # the task runs, and a psycopg connection isn't safe to use from
    # more than one thread at a time.
    owns_heartbeat_conn = heartbeat_conn is None
    if owns_heartbeat_conn:
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
        registered = get_registered_task(job["task"])
        if registered.wants_connection:
            # Exactly-once, not at-least-once: the task's writes and the
            # job's completion land in one commit, so a crash can't
            # leave the work done but unrecorded (or vice versa). This
            # is the case DESIGN.md's "no clean answer" argument doesn't
            # cover, because nothing here leaves the database.
            with conn.transaction():
                registered.fn(conn=conn, **job["args"])
                if not mark_succeeded_in_transaction(conn, job["id"], worker_id):
                    raise _LeaseLost
        else:
            registered.fn(**job["args"])
            mark_succeeded(conn, job["id"], worker_id)
    except _LeaseLost:
        pass  # another worker owns it now; their run is the one that counts
    except Exception as exc:
        mark_failed(conn, job["id"], worker_id, str(exc))
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join()
        if owns_heartbeat_conn:
            heartbeat_conn.close()

    return True


def run_worker(
        poll_interval: float = 1.0,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS,
        use_notify: bool = True) -> None:
    """
    use_notify=False falls back to pure polling. Correctness is
    identical either way - only idle-to-start latency differs, since
    polling has to wait out the interval before noticing new work.
    """
    conn = get_connection()
    heartbeat_conn = get_connection()
    worker_id = generate_worker_id()
    if use_notify:
        listen_for_jobs(conn)
    while True:
        if not process_one(
                conn, worker_id, lease_seconds, max_execution_seconds, heartbeat_conn):
            if use_notify:
                wait_for_job(conn, poll_interval)
            else:
                sleep(poll_interval)


if __name__ == "__main__":
    run_worker()
