import os
import queue
import socket
import threading
import uuid
from time import monotonic, sleep

import psycopg

from durable_queue.db import get_connection
from durable_queue.jobs import (
    DEFAULT_LEASE_SECONDS,
    JOB_NOTIFY_CHANNEL,
    claim_jobs,
    claim_next_job,
    extend_leases,
    mark_failed,
    mark_succeeded,
    mark_succeeded_in_transaction,
    reap_expired_jobs,
)

from durable_queue.registry import get_registered_task

DEFAULT_BATCH_SIZE = 10
DEFAULT_MAX_EXECUTION_SECONDS = 300


class _LeaseLost(Exception):
    """
    Raised to abort a transactional task whose lease lapsed mid-run.

    Raising inside the transaction rolls the task's own writes back,
    which is the point: if another worker has already reclaimed this
    job, our work must be discarded rather than committed alongside
    theirs.
    """


def relax_bookkeeping_durability(conn: psycopg.Connection) -> None:
    """
    Stop waiting for an fsync on this connection's commits.

    Sound for a worker's own bookkeeping, and only for that. The two
    commits a worker makes per job are the claim and the completion,
    and losing either in a crash is not data loss - the job reverts to
    pending and runs again, which is the at-least-once path this queue
    already implements, tests and documents. What must never be lost is
    an *enqueue*: work requested but silently never performed. That
    commit belongs to the caller's transaction, not this connection, so
    it stays durable regardless.

    The real cost is a higher chance of duplicate execution after a
    hard crash, since completions confirmed shortly before the crash
    can disappear. Tasks whose side effects are transactional (they
    take a `conn`) are unaffected - their writes vanish with the
    completion and are simply redone. Tasks with external side effects
    lean harder on their idempotency keys.
    """
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
    conn.commit()


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


class _Heartbeat:
    """
    One heartbeat thread for the whole worker, keeping every lease it
    holds alive.

    A thread per job cost ~0.13ms to create and join - around 3% of the
    per-job budget once batched claiming stopped dominating it. Tracking
    a deadline per job rather than per thread is also more correct: a
    hung job stops being extended without dragging down the others this
    worker is holding.
    """

    def __init__(self, conn: psycopg.Connection, worker_id: str,
                 lease_seconds: int, interval: float) -> None:
        self._conn = conn
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._interval = interval
        self._deadlines: dict[int, float | None] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def hold(self, job_ids: list[int], max_execution_seconds: float | None = None) -> None:
        """
        Keep these leases alive. max_execution_seconds=None means hold
        indefinitely, which is right for jobs claimed into the buffer
        but not started - their execution clock shouldn't run while
        they're waiting their turn.
        """
        deadline = None if max_execution_seconds is None else monotonic() + max_execution_seconds
        with self._lock:
            for job_id in job_ids:
                self._deadlines[job_id] = deadline

    def release(self, job_ids: list[int]) -> None:
        with self._lock:
            for job_id in job_ids:
                self._deadlines.pop(job_id, None)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()

    def _loop(self) -> None:
        # _stop.wait(interval) returns True the moment stop() is called
        # and False on timeout, so this extends on every timeout and
        # exits promptly when asked.
        while not self._stop.wait(self._interval):
            now = monotonic()
            with self._lock:
                # Dropping jobs past their deadline is what stops a hung
                # task being propped up forever: an unbounded heartbeat
                # defeats the reaper entirely, since a task blocked on a
                # network call with no timeout would hold its lease
                # indefinitely and never be recovered.
                live = [
                    job_id for job_id, deadline in self._deadlines.items()
                    if deadline is None or now < deadline
                ]
            extend_leases(self._conn, live, self._worker_id, self._lease_seconds)


def run_job(
        conn: psycopg.Connection,
        job: dict,
        worker_id: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS,
        heartbeat: "_Heartbeat | None" = None) -> None:
    """
    Run one already-claimed job and record its outcome.

    Pass the worker's heartbeat to reuse it; without one, a temporary
    heartbeat and connection are created for the duration of this job.
    Both cost real time per job (~13ms for a connection, ~0.13ms for a
    thread), so a worker should own them for its lifetime rather than
    paying per job.
    """
    # The heartbeat runs on its own connection in a background thread,
    # since the task call below blocks the main thread for as long as
    # the task runs, and a psycopg connection isn't safe to use from
    # more than one thread at a time.
    owns_heartbeat = heartbeat is None
    heartbeat_conn = None
    if owns_heartbeat:
        heartbeat_conn = get_connection()
        heartbeat = _Heartbeat(heartbeat_conn, worker_id, lease_seconds, lease_seconds / 3)
        heartbeat.start()

    # Starts this job's execution clock; it was held without a deadline
    # while it waited its turn in the worker's buffer.
    heartbeat.hold([job["id"]], max_execution_seconds)

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
        heartbeat.release([job["id"]])
        if owns_heartbeat:
            heartbeat.stop()
            heartbeat_conn.close()


def process_one(
        conn: psycopg.Connection,
        worker_id: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS,
        heartbeat: "_Heartbeat | None" = None) -> bool:
    """
    Claim and run a single job, if one is available.

    Returns True if a job was claimed (regardless of success/failure),
    False if the queue was empty.

    Reaping expired leases is deliberately not done here - run_worker
    sweeps on its own schedule. Doing it per job cost a commit and two
    UPDATEs for work that only matters once per lease period, and
    measured as roughly 90% of this library's overhead over raw
    Postgres.
    """
    job = claim_next_job(conn, worker_id, lease_seconds)
    if job is None:
        return False

    run_job(conn, job, worker_id, lease_seconds, max_execution_seconds, heartbeat)
    return True


def run_worker(
        poll_interval: float = 1.0,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS,
        use_notify: bool = True,
        reap_interval: float | None = None,
        durable_bookkeeping: bool = True,
        batch_size: int = DEFAULT_BATCH_SIZE,
        concurrency: int = 1) -> None:
    """
    use_notify=False falls back to pure polling. Correctness is
    identical either way - only idle-to-start latency differs, since
    polling has to wait out the interval before noticing new work.

    reap_interval defaults to a quarter of the lease, which is often
    enough to matter and rare enough to be free: an expired lease is
    only recoverable once it has actually expired, so sweeping far more
    frequently than the lease period buys nothing.

    durable_bookkeeping=False roughly doubles throughput on
    fsync-bound storage by not waiting for a flush on this worker's own
    claim and completion commits. Enqueues stay durable either way. See
    relax_bookkeeping_durability for exactly what that trades.

    batch_size controls how many jobs are claimed per round trip.
    Batching amortises the claim, which is otherwise a whole round trip
    per job (~1.7ms locally versus ~0.1ms at a batch of ten). Set it to
    1 to claim strictly on demand, which costs throughput but means a
    crash strands only one job and one worker can't scoop a short queue
    while others idle.

    concurrency is how many jobs this process runs at once. A worker
    spends most of each job waiting - on Postgres round trips here, and
    on HTTP calls in any realistic task - so overlapping jobs hides
    latency that no amount of query tuning can remove. Threads rather
    than async, because task functions are ordinary blocking Python and
    the GIL is released while they wait on IO.

    Each slot gets its own connection, since a psycopg connection can't
    be shared between threads, so a process holds concurrency + 2
    connections in total.
    """
    conn = get_connection()
    heartbeat_conn = get_connection()
    worker_id = generate_worker_id()

    # The claim is a single statement that doesn't need a transaction
    # wrapped around it, so an explicit COMMIT is a wasted round trip -
    # measured at ~47% of the claim's cost. The transactional task path
    # opens its own transaction explicitly, which works the same either
    # way.
    conn.autocommit = True

    if not durable_bookkeeping:
        relax_bookkeeping_durability(conn)
        relax_bookkeeping_durability(heartbeat_conn)
    if use_notify:
        listen_for_jobs(conn)
    if reap_interval is None:
        reap_interval = max(1.0, lease_seconds / 4)

    heartbeat = _Heartbeat(heartbeat_conn, worker_id, lease_seconds, lease_seconds / 3)
    heartbeat.start()

    # Never claim fewer jobs than there are slots to run them in, or
    # some slots would sit idle every batch.
    batch_size = max(batch_size, concurrency)
    # Refill before the queue empties rather than draining it first, so
    # slots never idle at a batch boundary and claiming overlaps
    # execution. This also bounds how many leases we hold at once.
    max_outstanding = max(batch_size, concurrency * 2)

    ready: queue.Queue = queue.Queue()
    outstanding = 0  # claimed but not yet finished: queued + running
    capacity = threading.Condition()

    def slot() -> None:
        nonlocal outstanding
        slot_conn = get_connection()
        slot_conn.autocommit = True
        if not durable_bookkeeping:
            relax_bookkeeping_durability(slot_conn)
        try:
            while True:
                job = ready.get()
                try:
                    if job is None:
                        return
                    run_job(
                        slot_conn, job, worker_id, lease_seconds,
                        max_execution_seconds, heartbeat,
                    )
                finally:
                    with capacity:
                        outstanding -= 1
                        capacity.notify()
                    ready.task_done()
        finally:
            slot_conn.close()

    for _ in range(concurrency):
        threading.Thread(target=slot, daemon=True).start()

    next_reap_at = 0.0  # sweep immediately on startup
    while True:
        if monotonic() >= next_reap_at:
            reap_expired_jobs(conn)
            next_reap_at = monotonic() + reap_interval

        with capacity:
            while outstanding >= max_outstanding:
                capacity.wait(timeout=0.5)
            room = max_outstanding - outstanding

        batch = claim_jobs(conn, worker_id, lease_seconds, min(room, batch_size))
        if batch:
            # Everything claimed is leased to us from this moment, so
            # the heartbeat has to keep the whole batch alive - not just
            # what's currently running - or jobs still queued get reaped
            # out from under us. No execution deadline yet: each job's
            # clock starts when a slot actually picks it up.
            heartbeat.hold([job["id"] for job in batch])
            with capacity:
                outstanding += len(batch)
            for job in batch:
                ready.put(job)
            continue

        with capacity:
            idle = outstanding == 0
        if idle:
            if use_notify:
                wait_for_job(conn, poll_interval)
            else:
                sleep(poll_interval)
        else:
            # Nothing left to claim, but slots are still working. Wait
            # for one to finish rather than spinning on an empty queue.
            with capacity:
                capacity.wait(timeout=0.05)


if __name__ == "__main__":
    run_worker()
