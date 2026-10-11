import logging
import os
import queue
import socket
import threading
import uuid
from contextlib import nullcontext
from time import monotonic
from typing import ContextManager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from durable_queue import wakeup
from durable_queue.db import (
    RECONNECT_MAX_DELAY,
    RECONNECT_MIN_DELAY,
    ReconnectingConnection,
    connection_kwargs,
    get_dsn,
)
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
    release_jobs,
    seconds_until_due,
)

from durable_queue.registry import PermanentError, get_registered_task

DEFAULT_BATCH_SIZE = 10
DEFAULT_MAX_EXECUTION_SECONDS = 300

logger = logging.getLogger(__name__)


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

    def __init__(self, conn: ReconnectingConnection, worker_id: str,
                 lease_seconds: int, interval: float, *, close_when_idle: bool = False) -> None:
        self._conn = conn
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._interval = interval
        self._close_when_idle = close_when_idle
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
            if not live:
                # Don't open a connection only to do nothing with it. In
                # idle mode, let go of an open one too, since the worker
                # may be about to sleep for hours.
                if self._close_when_idle:
                    self._conn.close()
                continue
            try:
                extend_leases(self._conn.get(), live, self._worker_id, self._lease_seconds)
            except Exception:
                # A dead heartbeat thread takes nothing else down with
                # it, which is exactly the danger: the worker carries on
                # claiming and running while every lease it holds
                # lapses, and its jobs are run a second time elsewhere.
                # So it never dies. It drops the connection and tries
                # again next beat; a lease outlasts three beats, so a
                # brief outage costs nothing.
                logger.warning(
                    "heartbeat: could not extend leases, retrying next beat", exc_info=True,
                )
                self._conn.close()


def _borrow(source: "psycopg.Connection | ConnectionPool") -> ContextManager[psycopg.Connection]:
    """
    A connection for one piece of database work: borrowed from the pool
    and returned when the block ends, or a plain connection as-is.
    """
    if isinstance(source, ConnectionPool):
        return source.connection()
    return nullcontext(source)


def run_job(
        conn: "psycopg.Connection | ConnectionPool",
        job: dict,
        worker_id: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        max_execution_seconds: float = DEFAULT_MAX_EXECUTION_SECONDS,
        heartbeat: "_Heartbeat | None" = None) -> None:
    """
    Run one already-claimed job and record its outcome.

    conn may be a pool, which is how run_worker's slots call this: a
    connection is then borrowed only for each piece of database work -
    the whole run for a transactional task, which needs it throughout,
    but only the completion write for any other task. Those spend their
    time waiting on things that aren't Postgres, and holding a
    connection through that wait is what made connections scale with
    slots rather than with database work.

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
        heartbeat_conn = ReconnectingConnection()
        heartbeat = _Heartbeat(heartbeat_conn, worker_id, lease_seconds, lease_seconds / 3)
        heartbeat.start()

    registered = None
    try:
        registered = get_registered_task(job["task"])

        # Starts this job's execution clock; it was held without a
        # deadline while it waited its turn in the worker's buffer.
        if registered.max_execution_seconds is not None:
            max_execution_seconds = registered.max_execution_seconds
        heartbeat.hold([job["id"]], max_execution_seconds)

        if registered.wants_connection:
            # Exactly-once, not at-least-once: the task's writes and the
            # job's completion land in one commit, so a crash can't
            # leave the work done but unrecorded (or vice versa). Only
            # possible because nothing here leaves the database.
            with _borrow(conn) as task_conn, task_conn.transaction():
                registered.fn(conn=task_conn, **job["args"])
                if not mark_succeeded_in_transaction(task_conn, job["id"], worker_id):
                    raise _LeaseLost
        else:
            registered.fn(**job["args"])
            with _borrow(conn) as done_conn:
                mark_succeeded(done_conn, job["id"], worker_id)
    except _LeaseLost:
        pass  # another worker owns it now; their run is the one that counts
    except Exception as exc:
        # registered is None only if the lookup itself failed - a worker
        # missing the task, typically mid-deploy. That one retries with
        # the defaults: a worker that has the task may claim it next.
        #
        # Borrowed afresh rather than reusing the connection above: if
        # that one broke, the pool has discarded it, so recording the
        # failure doesn't fail too.
        with _borrow(conn) as failed_conn:
            mark_failed(
                failed_conn, job["id"], worker_id, str(exc),
                permanent=isinstance(exc, PermanentError),
                max_attempts=registered.max_attempts if registered else None,
            )
        # A retry is new work with a time on it, which an idle worker
        # asleep until something later would otherwise sleep through.
        wakeup.wake()
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
        concurrency: int = 1,
        dsn: str | None = None,
        check_connections: bool = True,
        idle: bool = False,
        stop: threading.Event | None = None,
        shutdown_timeout: float = 30.0) -> None:
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

    Connections: two of the process's own (claiming and heartbeat), plus
    whatever the slots' pool has open - never more than concurrency.
    Slots borrow from it for each piece of database work rather than
    each owning a connection, which they used to hold idle through
    every HTTP call. For tasks that don't take `conn`, far fewer are in
    use at any moment, but the pool keeps what a burst made it open for
    10 idle minutes, so budget max_connections for concurrency + 2
    regardless. See run_job.

    dsn defaults to get_dsn(). The worker LISTENs, so it needs a direct
    connection, not one through a transaction-mode pooler; see get_dsn.

    A connection that drops is reopened on its next use, after a
    backoff; see ReconnectingConnection. check_connections has the
    slots' pool verify each connection before lending it, for a round
    trip per borrow. Without it, a connection the server closed while it
    sat idle in the pool - a restart, a database scaling to zero - is
    lent to a job, which fails and spends an attempt, or runs twice if
    the task had already finished. Only turn it off where connections
    never drop behind the worker's back.

    idle=True is for a database that scales to zero, such as Neon, which
    any polling keeps awake. Instead of polling, the worker works out
    when a job next needs it - a retry's run_at, a lease to recover -
    and sleeps until then without a query, closing its connections first
    if that's more than a minute off. Anything else that should wake it
    has to happen in this process and call wakeup.wake(), as a schedule
    firing on run_scheduler(idle=True) does, and an enqueue through
    durable_queue.sqlalchemy once it commits. An enqueue from another
    process waits for the next wake. LISTEN is off: the database would
    close its connection while asleep anyway. Runner sets all of this
    up.

    stop, once set, ends the worker. It stops claiming, hands back jobs
    it claimed but hadn't started, and gives running ones
    shutdown_timeout seconds to finish; any still running after that are
    left to lapse and be recovered elsewhere. Call wakeup.wake() after
    setting it, or an idle worker sleeps on until something else wakes
    it.
    """
    dsn = dsn or get_dsn()
    worker_id = generate_worker_id()
    if idle:
        use_notify = False

    def stopping() -> bool:
        return stop is not None and stop.is_set()

    def set_up_claim_conn(conn: psycopg.Connection) -> None:
        # The claim is a single statement that doesn't need a
        # transaction wrapped around it, so an explicit COMMIT is a
        # wasted round trip - measured at ~47% of the claim's cost. The
        # transactional task path opens its own transaction explicitly,
        # which works the same either way.
        conn.autocommit = True
        if not durable_bookkeeping:
            relax_bookkeeping_durability(conn)
        if use_notify:
            listen_for_jobs(conn)

    claim_conn = ReconnectingConnection(dsn, set_up_claim_conn)
    heartbeat_conn = ReconnectingConnection(
        dsn, None if durable_bookkeeping else relax_bookkeeping_durability,
    )
    # Connect once up front, so a bad DSN fails at startup rather than
    # being retried forever. From here on, a dropped connection is
    # reopened when it's next needed.
    claim_conn.get()

    if reap_interval is None:
        reap_interval = max(1.0, lease_seconds / 4)

    heartbeat = _Heartbeat(
        heartbeat_conn, worker_id, lease_seconds, lease_seconds / 3, close_when_idle=idle,
    )
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

    def open_slot_pool() -> ConnectionPool:
        # A slot borrows at most one connection at a time, so
        # concurrency is enough for every slot to be mid-transaction at
        # once and no slot ever waits on a connection another is
        # holding - at most on one being opened. It only grows that far
        # if slots really are using them together: the pool starts at
        # one and opens another only when a borrower would otherwise
        # have to wait.
        return ConnectionPool(
            dsn,
            kwargs={"row_factory": dict_row, "autocommit": True, **connection_kwargs(dsn)},
            configure=None if durable_bookkeeping else relax_bookkeeping_durability,
            check=ConnectionPool.check_connection if check_connections else None,
            min_size=1,
            max_size=concurrency,
            open=True,
        )

    # In idle mode, opened when there's work and closed again before a
    # long sleep.
    slot_pool: ConnectionPool | None = None if idle else open_slot_pool()

    def slot() -> None:
        nonlocal outstanding
        while True:
            job = ready.get()
            try:
                if job is None:
                    return
                run_job(
                    slot_pool, job, worker_id, lease_seconds,
                    max_execution_seconds, heartbeat,
                )
            except Exception:
                # run_job records a task's failure itself, so getting
                # here means recording it failed too: the database was
                # unreachable, or longer than the pool will wait. The job
                # needs nothing more - its lease is no longer extended,
                # so it lapses and the job is reaped and run again - but
                # the slot must survive. One that died here left its
                # share of claimed jobs in the buffer with their leases
                # still extended, and once every slot had gone the
                # worker held all of them and ran none.
                logger.exception(
                    "worker %s: could not record the outcome of job %s", worker_id, job["id"],
                )
            finally:
                with capacity:
                    outstanding -= 1
                    finished_last = outstanding == 0
                    capacity.notify()
                ready.task_done()
                if idle and finished_last and job is not None:
                    # The end of a burst. An idle worker sleeps until its
                    # running jobs' leases would lapse, so it has to be
                    # told it can go back to sleeping properly now -
                    # and let go of its connections.
                    wakeup.wake()

    slots = [threading.Thread(target=slot, daemon=True) for _ in range(concurrency)]
    for thread in slots:
        thread.start()

    retry_delay = RECONNECT_MIN_DELAY
    next_reap_at = 0.0  # sweep immediately on startup
    while not stopping():
        # Read before looking for work, so that a wake() landing between
        # finding none and going to sleep isn't slept through.
        seen = wakeup.current()
        try:
            if monotonic() >= next_reap_at:
                reap_expired_jobs(claim_conn.get())
                next_reap_at = monotonic() + reap_interval

            with capacity:
                while outstanding >= max_outstanding and not stopping():
                    capacity.wait(timeout=0.5)
                room = max_outstanding - outstanding
            if stopping():
                break

            batch = claim_jobs(claim_conn.get(), worker_id, lease_seconds, min(room, batch_size))
            retry_delay = RECONNECT_MIN_DELAY
            if batch:
                # Everything claimed is leased to us from this moment, so
                # the heartbeat has to keep the whole batch alive - not
                # just what's currently running - or jobs still queued
                # get reaped out from under us. No execution deadline
                # yet: each job's clock starts when a slot actually picks
                # it up.
                heartbeat.hold([job["id"] for job in batch])
                if slot_pool is None:
                    slot_pool = open_slot_pool()
                with capacity:
                    outstanding += len(batch)
                for job in batch:
                    ready.put(job)
                continue

            with capacity:
                nothing_outstanding = outstanding == 0
            if idle:
                seconds = seconds_until_due(claim_conn.get())
                if seconds is not None and seconds <= 0:
                    # Due, yet nothing to claim: an expired lease the
                    # reaper hasn't swept yet, or a job another worker
                    # has locked. Sweep, and look again shortly.
                    next_reap_at = 0.0
                timeout = None if seconds is None else max(seconds, wakeup.MIN_SLEEP)
                if nothing_outstanding and (
                        timeout is None or timeout > wakeup.CLOSE_CONNECTIONS_AFTER):
                    # A long sleep: let go of every connection (the
                    # heartbeat lets go of its own), so there's nothing
                    # for a database scaling to zero to close under us.
                    claim_conn.close()
                    if slot_pool is not None:
                        slot_pool.close()
                        slot_pool = None
                wakeup.wait(seen, timeout)
            elif nothing_outstanding:
                if use_notify:
                    wait_for_job(claim_conn.get(), poll_interval)
                else:
                    wakeup.pause(poll_interval, stop)
            else:
                # Nothing left to claim, but slots are still working.
                # Wait for one to finish rather than spinning on an
                # empty queue.
                with capacity:
                    capacity.wait(timeout=0.05)
        except psycopg.OperationalError:
            # Whatever the connection was doing is safe to repeat. A
            # claim that never committed claimed nothing, and one that
            # committed unseen leaves leases that lapse and are reaped.
            # Notifications sent while it was down are lost, but the
            # claim right after reconnecting finds that work anyway.
            logger.warning(
                "worker %s: lost its database connection, retrying in %.0fs",
                worker_id, retry_delay, exc_info=True,
            )
            claim_conn.close()
            wakeup.pause(retry_delay, stop)
            retry_delay = min(retry_delay * 2, RECONNECT_MAX_DELAY)

    # Stopping. Jobs claimed but not started go back to the queue for
    # whoever runs next, rather than waiting out their leases.
    unstarted = []
    while True:
        try:
            unstarted.append(ready.get_nowait()["id"])
        except queue.Empty:
            break
        ready.task_done()
    if unstarted:
        heartbeat.release(unstarted)
        try:
            release_jobs(claim_conn.get(), unstarted, worker_id)
        except psycopg.OperationalError:
            logger.warning(
                "worker %s: could not hand back %d unstarted jobs; they're recovered"
                " once their leases lapse", worker_id, len(unstarted), exc_info=True,
            )

    for _ in slots:
        ready.put(None)
    deadline = monotonic() + shutdown_timeout
    for thread in slots:
        thread.join(max(0.0, deadline - monotonic()))
    heartbeat.stop()
    if slot_pool is not None:
        slot_pool.close()
    claim_conn.close()
    heartbeat_conn.close()

if __name__ == "__main__":
    run_worker()
