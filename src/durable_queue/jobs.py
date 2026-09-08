import random
from datetime import timedelta

import psycopg
from psycopg.types.json import Jsonb

DEFAULT_LEASE_SECONDS = 30

# Workers LISTEN on this channel so an enqueue can wake an idle worker
# immediately instead of it waiting out a poll interval.
JOB_NOTIFY_CHANNEL = "durable_queue_jobs"


def enqueue(
        conn: psycopg.Connection,
        task: str,
        args: dict,
        *,
        idempotency_key: str | None = None) -> int:
    """
    Insert a new job and return its id.

    If idempotency_key is given and a job with that key already exists,
    no new row is created - the existing job's id is returned instead of
    raising. This is what lets a caller do "enqueue this if it isn't
    already enqueued" without having to catch a database error, which
    matters when the caller enqueuing it might itself be retried.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (task, args, idempotency_key)
            VALUES (%s, %s, %s)
            ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
            RETURNING id
            """,
            (task, Jsonb(args), idempotency_key),
        )
        new_id = cur.fetchone()["id"]

        # Deliberately inside the caller's transaction: Postgres only
        # delivers a NOTIFY when that transaction commits, so a job
        # enqueued in a transaction that rolls back never wakes anyone.
        # Identical notifications within one transaction are collapsed,
        # so a bulk fan-out costs one wakeup, not one per job.
        cur.execute("SELECT pg_notify(%s, '')", (JOB_NOTIFY_CHANNEL,))
    return new_id


def claim_next_job(
        conn: psycopg.Connection,
        worker_id: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'running', locked_by = %s, locked_until = now() + %s
            WHERE id = (
                SELECT id FROM jobs
                WHERE status = 'pending' AND run_at <= now()
                ORDER BY run_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, task, args
            """,
            (worker_id, timedelta(seconds=lease_seconds)),
        )
        row = cur.fetchone()
    # Commit even when nothing was claimed. Returning early without a
    # commit leaves this connection "idle in transaction" for the whole
    # poll interval, which pins Postgres's xmin horizon and stops
    # autovacuum from reclaiming dead tuples anywhere in the database -
    # a worker sitting on an empty queue would quietly sabotage the very
    # bloat behaviour M10 exists to measure.
    conn.commit()
    return row


def extend_lease(
        conn: psycopg.Connection,
        job_id: int,
        worker_id: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS) -> None:
    """
    Heartbeat: push a job's lease further into the future.

    Guarded by locked_by so a worker that has already lost this job
    (reaped and reclaimed by someone else) can't clobber the new
    owner's lease.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET locked_until = now() + %s
            WHERE id = %s AND locked_by = %s AND status = 'running'
            """,
            (timedelta(seconds=lease_seconds), job_id, worker_id),
        )
    conn.commit()


def reap_expired_jobs(conn: psycopg.Connection) -> int:
    """
    Return any job whose lease expired back to pending, and dead-letter
    the ones that keep taking their worker down with them. Returns the
    total count reaped, recovered and dead-lettered together.

    A job that crashes its worker process outright never reaches
    mark_failed, so attempts never increments and max_attempts can
    never stop it - reap, claim, crash, forever. Counting recoveries
    separately from failures is what bounds that: a crash isn't a
    failure, but enough of them is still a poison pill.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'dead', finished_at = now(), recoveries = recoveries + 1,
                locked_by = NULL, locked_until = NULL,
                last_error = 'worker repeatedly died while running this job'
            WHERE status = 'running' AND locked_until < now()
              AND recoveries + 1 >= max_recoveries
            """
        )
        dead_lettered = cur.rowcount

        cur.execute(
            """
            UPDATE jobs
            SET status = 'pending', recoveries = recoveries + 1,
                locked_by = NULL, locked_until = NULL
            WHERE status = 'running' AND locked_until < now()
            """
        )
        recovered = cur.rowcount
    conn.commit()
    return dead_lettered + recovered


def mark_succeeded_in_transaction(conn: psycopg.Connection, job_id: int, worker_id: str) -> bool:
    """
    The mark_succeeded UPDATE without the commit, for a caller running
    it inside its own transaction - specifically a transactional task,
    whose writes have to land in the same commit as the job's
    completion. Returns whether this worker still held the job.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs SET status = 'succeeded', finished_at = now()
            WHERE id = %s AND locked_by = %s AND status = 'running'
            """,
            (job_id, worker_id),
        )
        return cur.rowcount > 0


def mark_succeeded(conn: psycopg.Connection, job_id: int, worker_id: str) -> bool:
    """
    Mark a job succeeded, but only if this worker still owns its lease.

    Guarded by locked_by for the same reason extend_lease is: a worker
    that lost the job (its lease lapsed and someone else reclaimed it)
    must not write a terminal status over the new owner's in-flight
    work. Returns whether this worker still held the job.
    """
    still_owned = mark_succeeded_in_transaction(conn, job_id, worker_id)
    conn.commit()
    return still_owned


def compute_backoff(attempts: int, *, base_seconds: float = 1.0, max_seconds: float = 300.0) -> float:
    """
    Exponential backoff with full jitter: a delay chosen uniformly from
    [0, min(max_seconds, base_seconds * 2**attempts)].

    The jitter is what actually prevents a thundering herd, not the
    exponential growth by itself — 500 jobs that all fail at the same
    instant would, without it, all compute the exact same delay and all
    retry at the exact same instant again. Picking randomly within the
    range spreads the retries out instead of just moving them later.
    """
    upper_bound = min(max_seconds, base_seconds * (2 ** attempts))
    return random.uniform(0, upper_bound)


def mark_failed(conn: psycopg.Connection, job_id: int, worker_id: str, error: str) -> bool:
    """
    Record a failure. Reschedules to 'pending' with a backed-off run_at
    if attempts remain, otherwise flips to 'dead'. Either way the job's
    lease is released (locked_by/locked_until cleared) since it's no
    longer owned by a running worker.

    Guarded by locked_by like mark_succeeded: a worker whose lease
    lapsed must not reschedule a job that another worker has already
    reclaimed and is actively running - that would hand the same job
    out a third time. Returns whether this worker still held the job.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs SET attempts = attempts + 1
            WHERE id = %s AND locked_by = %s AND status = 'running'
            RETURNING attempts, max_attempts
            """,
            (job_id, worker_id),
        )
        row = cur.fetchone()
        if row is None:
            conn.commit()
            return False

        if row["attempts"] >= row["max_attempts"]:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'dead', finished_at = now(), last_error = %s,
                    locked_by = NULL, locked_until = NULL
                WHERE id = %s
                """,
                (error, job_id),
            )
        else:
            delay = compute_backoff(row["attempts"])
            cur.execute(
                """
                UPDATE jobs
                SET status = 'pending', run_at = now() + %s, last_error = %s,
                    locked_by = NULL, locked_until = NULL
                WHERE id = %s
                """,
                (timedelta(seconds=delay), error, job_id),
            )
    conn.commit()
    return True


def get_job(conn: psycopg.Connection, job_id: int) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        return cur.fetchone()


def list_jobs(conn: psycopg.Connection, *, status: str | None = None, limit: int = 20) -> list[dict]:
    with conn.cursor() as cur:
        if status is not None:
            cur.execute(
                """
                SELECT id, task, status, attempts, run_at, created_at, last_error
                FROM jobs WHERE status = %s ORDER BY created_at DESC LIMIT %s
                """,
                (status, limit),
            )
        else:
            cur.execute(
                """
                SELECT id, task, status, attempts, run_at, created_at, last_error
                FROM jobs ORDER BY created_at DESC LIMIT %s
                """,
                (limit,),
            )
        return cur.fetchall()


def retry_job(conn: psycopg.Connection, job_id: int) -> bool:
    """
    Reset a dead job back to pending with a clean slate: attempts reset
    to 0, so it gets a full fresh set of max_attempts tries. Only ever
    touches a job currently in 'dead' status - retrying a pending,
    running, or already-succeeded job isn't a meaningful operation.
    last_error is left alone as a record of what failed last time;
    it'll be overwritten if the retry fails again. Returns whether a
    row was actually reset.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'pending', attempts = 0, recoveries = 0,
                run_at = now(), finished_at = NULL
            WHERE id = %s AND status = 'dead'
            """,
            (job_id,),
        )
        updated = cur.rowcount > 0
    conn.commit()
    return updated


def get_queue_stats(conn: psycopg.Connection) -> dict:
    """
    Counts by status, plus the age of the oldest due-but-unclaimed
    pending job (status='pending' AND run_at <= now() - the same
    condition claim_next_job itself uses). That's the number worth
    alerting on, not raw queue depth: a depth of 10,000 draining in a
    second is fine, a depth of 3 stuck for an hour is an outage. A job
    scheduled for later via a future run_at is excluded - it isn't
    stuck, it just isn't due yet.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT status, count(*) AS n FROM jobs GROUP BY status")
        counts = {row["status"]: row["n"] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT extract(epoch FROM now() - run_at) AS age_seconds
            FROM jobs WHERE status = 'pending' AND run_at <= now()
            ORDER BY run_at LIMIT 1
            """
        )
        oldest = cur.fetchone()

    return {
        "pending": counts.get("pending", 0),
        "running": counts.get("running", 0),
        "succeeded": counts.get("succeeded", 0),
        "dead": counts.get("dead", 0),
        "oldest_pending_age_seconds": oldest["age_seconds"] if oldest else None,
    }
