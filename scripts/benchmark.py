"""
Benchmark harness for durable-queue.

Measures two things against a real local Postgres:

  throughput  - how many jobs/sec N worker processes drain from a
                pre-filled queue.
  latency     - how long a job waits between being enqueued and
                starting to run, with workers already idle. Reported
                for LISTEN/NOTIFY and for pure polling, since that gap
                is the entire argument for NOTIFY.

Run it:  python scripts/benchmark.py
         python scripts/benchmark.py --jobs 2000 --workers 4

Requires Postgres up and the schema applied (see README).
"""
import argparse
import os
import random
import statistics
import subprocess
import sys
import time

from urllib.parse import quote

import psycopg
from psycopg.rows import dict_row

from durable_queue.db import get_connection, get_dsn
from durable_queue.jobs import enqueue
from durable_queue.registry import task
from durable_queue.jobs import DEFAULT_LEASE_SECONDS, claim_jobs, enqueue_many
from durable_queue.worker import _Heartbeat, generate_worker_id, run_job, run_worker

WORKER_SETTLE_SECONDS = 1.5


@task
def bench_job(conn, enqueued_at: float) -> None:
    """
    Declares `conn`, so it runs inside the worker's transaction: the
    sample insert commits with the job's completion and costs no extra
    connection, which keeps the harness from measuring its own
    overhead instead of the queue's.
    """
    latency_ms = (time.time() - enqueued_at) * 1000.0
    with conn.cursor() as cur:
        cur.execute("INSERT INTO bench_samples (latency_ms) VALUES (%s)", (latency_ms,))


IO_JOB_SECONDS = 0.02


@task
def io_job(enqueued_at: float) -> None:
    """
    The shape of most real tasks: no `conn`, and nearly all of its time
    spent waiting on something that isn't Postgres - an HTTP call,
    stood in for by a sleep. Whether a worker holds a connection through
    that wait is exactly what the connections suite measures.
    """
    time.sleep(IO_JOB_SECONDS)


@task
def io_job_varied(enqueued_at: float) -> None:
    """
    io_job with the variance real HTTP latency has - same mean, spread
    over 10-30ms. Jobs claimed in one batch start together; with a fixed
    sleep they also finish together, and all borrow a connection at the
    same instant, which no real workload does.
    """
    time.sleep(random.uniform(IO_JOB_SECONDS / 2, IO_JOB_SECONDS * 1.5))


def _reset(conn) -> None:
    """
    Empty the tables and force a checkpoint, so every run starts from
    the same state.

    The checkpoint is there because without it the timed one, every 5
    minutes and taking 5-47s to complete here, landed in whichever runs
    happened to overlap it. Every variant in that round came out up to
    ~30% slow, which is larger than most of the differences being
    measured. Forcing one now also restarts the 5-minute timer, so none
    fires mid-run. CHECKPOINT needs superuser or pg_checkpoint, which
    the docker-compose role has.
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE jobs, bench_samples")
    conn.commit()
    conn.execute("CHECKPOINT")
    conn.commit()


def _spawn_workers(count: int, *, use_notify: bool, poll_interval: float,
                   dsn: str | None = None, concurrency: int = 1,
                   batch_size: int = 10) -> list:
    argv = [
        sys.executable, __file__, "--worker",
        "--poll-interval", str(poll_interval),
        "--concurrency", str(concurrency),
        "--batch-size", str(batch_size),
    ]
    if not use_notify:
        argv.append("--no-notify")
    env = None
    if dsn is not None:
        env = {**os.environ, "DATABASE_URL": dsn}
    workers = [
        subprocess.Popen(argv, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(count)
    ]
    time.sleep(WORKER_SETTLE_SECONDS)  # don't bill process startup to the queue
    return workers


def _stop(workers: list) -> None:
    for w in workers:
        w.kill()
    for w in workers:
        w.wait()


def _any_unfinished(conn) -> bool:
    """
    Progress check for the drain loop, phrased to match the partial
    indexes exactly so it becomes two index-only scans rather than a
    sequential scan.

    The obvious phrasing - count(*) WHERE status NOT IN ('succeeded',
    'dead') - is a seq scan costing 2.85ms on a 30k-row table, run 20
    times a second while measuring. That grows with the table, so it
    quietly penalised exactly the long runs it was meant to measure.
    This version is 0.086ms and O(1) in table size.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE status = 'pending')"
            "    OR EXISTS(SELECT 1 FROM jobs WHERE status = 'running') AS unfinished"
        )
        unfinished = cur.fetchone()["unfinished"]
    conn.commit()
    return unfinished


def _pending_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM jobs WHERE status NOT IN ('succeeded', 'dead')")
        count = cur.fetchone()["n"]
    # Ending the transaction matters more than it looks: a bare SELECT
    # still opens one, and leaving it open holds an ACCESS SHARE lock on
    # jobs, which blocks the next phase's TRUNCATE indefinitely. Each
    # phase works in isolation; only the sequence deadlocks.
    conn.commit()
    return count


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(int(len(ordered) * pct), len(ordered) - 1)
    return ordered[index]


def _worker_connection_count(conn) -> tuple[int, int]:
    """
    Every connection to this database except the harness's own, as
    (open, busy). Busy is anything not idle - a statement running or a
    commit waiting on its flush - which is what a pool actually has to
    supply; open also counts what the pool is merely keeping around.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n, count(*) FILTER (WHERE state <> 'idle') AS busy"
            " FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )
        row = cur.fetchone()
    conn.commit()
    return row["n"], row["busy"]


def _drain(conn, *, timeout_seconds: float = 120.0, on_poll=None) -> float:
    """Wait for the queue to empty, returning elapsed seconds."""
    started = time.monotonic()
    deadline = started + timeout_seconds
    while True:
        if on_poll is not None:
            on_poll()
        if not _any_unfinished(conn):
            return time.monotonic() - started
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"queue did not drain within {timeout_seconds}s; "
                f"{_pending_count(conn)} jobs left"
            )
        time.sleep(0.05)


# Set by --repeat. Every throughput figure is the median of this many runs.
REPEAT = 1


def measure_throughput(conn, **options) -> float:
    """
    The median of REPEAT runs of _measure_throughput_once.

    One run at 32-64 jobs in flight can still come out ~15% low, with
    every other run of the same configuration agreeing. The likely
    cause is autovacuum: it runs on jobs about once a minute under this
    load, and each run starts from a truncated table Postgres believes
    is empty, so its trigger threshold is near zero. Turning it off
    would flatter the numbers - a real queue table is vacuumed
    constantly - so instead no single run gets to decide a figure.
    """
    return statistics.median(_measure_throughput_once(conn, **options) for _ in range(REPEAT))


def _measure_throughput_once(conn, *, job_count: int, worker_count: int, poll_interval: float,
                       dsn: str | None = None, concurrency: int = 1,
                       batch_size: int = 10, use_notify: bool = True,
                       task_name: str = "bench_job", connections: list | None = None) -> float:
    """
    Pass a list as connections to have it filled with the workers'
    (open, busy) connection counts, sampled at every drain poll.

    Timed on the database's clock, from just after the enqueue commits
    to the last job's finished_at, rather than by when the drain loop
    noticed the queue was empty. That loop polls every 50ms, so a run
    that drains in two seconds was only ever timed to within ±2.5%.
    """
    _reset(conn)
    workers = _spawn_workers(
        worker_count, use_notify=use_notify, poll_interval=poll_interval, dsn=dsn,
        concurrency=concurrency, batch_size=batch_size,
    )
    on_poll = None
    if connections is not None:
        def on_poll():
            connections.append(_worker_connection_count(conn))
    try:
        now = time.time()
        enqueue_many(conn, task_name, [{"enqueued_at": now}] * job_count)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT clock_timestamp() AS started_at")
            started_at = cur.fetchone()["started_at"]
        conn.commit()
        _drain(conn, on_poll=on_poll)
    finally:
        _stop(workers)
    with conn.cursor() as cur:
        cur.execute("SELECT max(finished_at) AS finished_at FROM jobs")
        finished_at = cur.fetchone()["finished_at"]
    conn.commit()
    return job_count / (finished_at - started_at).total_seconds()


def _jobs_for(base: int, in_flight: int) -> int:
    """
    A run's job count, scaled with how many jobs it runs at once, so a
    fast configuration doesn't drain in under a second and a slow one
    doesn't take minutes. Every run then lasts long enough for startup -
    the pool growing, the first claims, the burst of full-page writes
    after the forced checkpoint - to be a small share of it.
    """
    return base * max(1, in_flight // 4)


def measure_latency(conn, *, samples: int, worker_count: int, poll_interval: float,
                    use_notify: bool) -> list[float]:
    _reset(conn)
    workers = _spawn_workers(worker_count, use_notify=use_notify, poll_interval=poll_interval)
    try:
        for _ in range(samples):
            # One job at a time, with a gap, so workers are genuinely
            # idle when it lands - that's the case NOTIFY changes.
            enqueue(conn, "bench_job", {"enqueued_at": time.time()})
            conn.commit()
            time.sleep(poll_interval * 1.5)

        _drain(conn, timeout_seconds=30.0)

        with conn.cursor() as cur:
            cur.execute("SELECT latency_ms FROM bench_samples")
            return [row["latency_ms"] for row in cur.fetchall()]
    finally:
        _stop(workers)


def measure_raw_ceiling(conn, *, job_count: int) -> float:
    """
    The same claim/work/complete cycle with no library in the path: what
    Postgres itself can sustain, single process. The gap against
    measure_single_process_library is durable-queue's overhead - leases,
    the reaper sweep, registry dispatch, the heartbeat thread.
    """
    _reset(conn)
    with conn.cursor() as cur:
        for _ in range(job_count):
            cur.execute("INSERT INTO jobs (task, args) VALUES ('raw', '{}')")
    conn.commit()

    started = time.monotonic()
    processed = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET status = 'running', locked_by = 'raw'
                WHERE id = (
                    SELECT id FROM jobs
                    WHERE status = 'pending' AND run_at <= now()
                    ORDER BY run_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id
                """
            )
            row = cur.fetchone()
        conn.commit()
        if row is None:
            break

        with conn.cursor() as cur:
            cur.execute("INSERT INTO bench_samples (latency_ms) VALUES (0)")
            cur.execute(
                "UPDATE jobs SET status = 'succeeded', finished_at = now() WHERE id = %s",
                (row["id"],),
            )
        conn.commit()
        processed += 1

    return processed / (time.monotonic() - started)


def measure_single_process_library(conn, *, job_count: int, batch_size: int) -> float:
    """
    durable-queue draining the same queue in one process, taking the
    same path run_worker does - batched claims on an autocommit
    connection - so this measures what a real worker costs.
    """
    _reset(conn)
    now = time.time()
    for _ in range(job_count):
        enqueue(conn, "bench_job", {"enqueued_at": now})
    conn.commit()

    worker_conn = get_connection()
    worker_conn.autocommit = True
    heartbeat_conn = get_connection()
    worker_id = generate_worker_id()
    heartbeat = _Heartbeat(heartbeat_conn, worker_id, DEFAULT_LEASE_SECONDS,
                           DEFAULT_LEASE_SECONDS / 3)
    heartbeat.start()
    try:
        started = time.monotonic()
        while True:
            batch = claim_jobs(worker_conn, worker_id, batch_size=batch_size)
            if not batch:
                break
            heartbeat.hold([job["id"] for job in batch])
            while batch:
                run_job(worker_conn, batch.pop(0), worker_id, heartbeat=heartbeat)
        elapsed = time.monotonic() - started
    finally:
        heartbeat.stop()
        heartbeat_conn.close()
        worker_conn.close()
    return job_count / elapsed


def _dsn_with(**settings: str) -> str:
    """
    The same database with per-connection settings applied through the
    DSN rather than ALTER DATABASE. Nothing global changes, so there's
    no shared state to leak into the test suite and nothing to reset if
    this process dies partway through.
    """
    base = get_dsn()
    options = quote(" ".join(f"-c {key}={value}" for key, value in settings.items()), safe="")
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}options={options}"


def _relaxed_dsn() -> str:
    return _dsn_with(synchronous_commit="off")


def _measure_with_dsn(dsn: str, *, job_count: int, worker_count: int,
                      poll_interval: float, **worker_options) -> float:
    tuned_conn = psycopg.connect(dsn, row_factory=dict_row)
    try:
        return measure_throughput(
            tuned_conn, job_count=job_count, worker_count=worker_count,
            poll_interval=poll_interval, dsn=dsn, **worker_options,
        )
    finally:
        tuned_conn.close()


# How a worker behaves before any of the tuning work: one job claimed
# per round trip, one at a time, and no notification to wake it.
NAIVE = {"concurrency": 1, "batch_size": 1, "use_notify": False}
TUNED = {"concurrency": 8, "batch_size": 25, "use_notify": True}


def run_comparison_suite(conn, *, job_count: int, poll_interval: float) -> None:
    """
    Naive versus tuned, measured back to back on the same machine.

    "Naive" is one job claimed per round trip, run one at a time, woken
    by polling. Reaper throttling, the reused heartbeat connection and
    the shared heartbeat thread are unconditional and apply to both
    columns, so this isolates batching, slots and NOTIFY.
    """
    print()
    print("  throughput (jobs/sec)      naive      tuned    speedup")
    for workers in (1, 2, 4, 8):
        naive = measure_throughput(
            conn, job_count=_jobs_for(job_count, workers), worker_count=workers,
            poll_interval=poll_interval, **NAIVE,
        )
        tuned = measure_throughput(
            conn, job_count=_jobs_for(job_count, workers * TUNED["concurrency"]),
            worker_count=workers, poll_interval=poll_interval, **TUNED,
        )
        print(f"  {workers} worker process(es){naive:>11.0f}{tuned:>11.0f}   {tuned / naive:>6.1f}x")

    naive_latency = measure_latency(
        conn, samples=10, worker_count=4, poll_interval=poll_interval, use_notify=False
    )
    tuned_latency = measure_latency(
        conn, samples=10, worker_count=4, poll_interval=poll_interval, use_notify=True
    )
    naive_p50 = _percentile(naive_latency, 0.50)
    tuned_p50 = _percentile(tuned_latency, 0.50)
    print()
    print("  enqueue -> start (ms)      naive      tuned    speedup")
    print(f"  p50 latency        {naive_p50:>11.1f}{tuned_p50:>11.1f}   "
          f"{naive_p50 / max(tuned_p50, 0.001):>6.1f}x")


def run_curve_suite(conn, *, job_count: int, poll_interval: float) -> None:
    """
    Throughput against total jobs in flight, which is what actually
    determines it - not how the concurrency is split between processes
    and threads.

    Reading a scaling number without knowing where on this curve it
    sits is how you conclude that one configuration "scales better"
    than another when both are simply walking different ranges of the
    same curve.

    The second table holds jobs in flight at 8 and varies only the
    split, which is the direct test of that claim.
    """
    def rate_for(workers: int, slots: int) -> float:
        return measure_throughput(
            conn, job_count=_jobs_for(job_count, workers * slots), worker_count=workers,
            poll_interval=poll_interval, concurrency=slots, batch_size=25,
        )

    print()
    print(f"  {'in flight':>10} {'split':>10} {'jobs/sec':>10} {'marginal':>10}")
    previous = None
    for workers, slots in ((1, 1), (1, 2), (1, 4), (1, 8), (2, 8), (4, 8), (8, 8)):
        rate = rate_for(workers, slots)
        marginal = "-" if previous is None else f"{rate / previous:.2f}x"
        split = f"{workers}w x {slots}s"
        print(f"  {workers * slots:>10} {split:>10} {rate:>10.0f} {marginal:>10}")
        previous = rate

    print()
    print(f"  {'in flight':>10} {'split':>10} {'jobs/sec':>10}")
    for workers, slots in ((8, 1), (4, 2), (2, 4), (1, 8)):
        print(f"  {8:>10} {f'{workers}w x {slots}s':>10} {rate_for(workers, slots):>10.0f}")


def run_connections_suite(conn, *, job_count: int, poll_interval: float) -> None:
    """
    Postgres connections held by the workers, alongside throughput.

    io_job holds no transaction and spends its time waiting on something
    other than Postgres, so a connection held through it is idle. bench_job
    is transactional and needs its connection for the whole run, so it is
    the check that pooling costs nothing where it can't help.

    Peak open is what to budget max_connections against. Average busy is
    what the work actually needed; the gap is connections a pool opened
    for a burst and is keeping until they've sat idle long enough.
    """
    print()
    print(f"  {'task':>13} {'split':>9} {'jobs/sec':>9} {'peak open':>10} {'avg busy':>9}")
    # io_job's rate is set by its sleep rather than by the queue, so its
    # job counts are sized by hand to keep each run to several seconds.
    for task_name, workers, slots, jobs in (
            ("io_job", 1, 8, job_count), ("io_job", 1, 32, job_count * 2),
            ("io_job_varied", 1, 32, job_count * 2), ("io_job", 4, 32, job_count * 4),
            ("bench_job", 1, 8, _jobs_for(job_count, 8)),
            ("bench_job", 4, 8, _jobs_for(job_count, 32))):
        samples: list[tuple[int, int]] = []
        rate = measure_throughput(
            conn, job_count=jobs, worker_count=workers,
            poll_interval=poll_interval, concurrency=slots, batch_size=25,
            task_name=task_name, connections=samples,
        )
        split = f"{workers}w x {slots}s"
        peak_open = max(opened for opened, _ in samples)
        avg_busy = sum(busy for _, busy in samples) / len(samples)
        print(f"  {task_name:>13} {split:>9} {rate:>9.0f} {peak_open:>10} {avg_busy:>9.1f}")


def run_concurrency_suite(conn, *, job_count: int, worker_count: int,
                          poll_interval: float) -> None:
    """
    Slots per worker process. A worker spends most of each job waiting
    on a round trip, so overlapping jobs hides latency that no query
    tuning can remove. bench_job is database-bound, which understates
    this - a task waiting on an HTTP call would gain far more.
    """
    print()
    print(f"  slots/worker   jobs/sec   vs 1 slot   ({worker_count} worker processes)")
    single = None
    for slots in (1, 2, 4, 8):
        rate = measure_throughput(
            conn, job_count=job_count, worker_count=worker_count,
            poll_interval=poll_interval, concurrency=slots,
        )
        if single is None:
            single = rate
        print(f"  {slots:>12}   {rate:>8.0f}   {rate / single:>8.2f}x")


def run_commit_delay_suite(conn, *, job_count: int, worker_count: int,
                           poll_interval: float) -> None:
    """
    commit_delay makes a committing backend pause briefly so other
    concurrent commits can join the same flush. Unlike
    synchronous_commit=off it gives up no durability at all - every
    commit is still fsynced - it just amortises the fsync across
    several transactions, which is only possible because several
    workers are committing at once.
    """
    print()
    print("  commit_delay    jobs/sec   change")
    baseline = None
    for delay_us in (0, 500, 1000, 2000):
        if delay_us == 0:
            rate = measure_throughput(
                conn, job_count=job_count, worker_count=worker_count,
                poll_interval=poll_interval,
            )
            baseline = rate
        else:
            rate = _measure_with_dsn(
                _dsn_with(commit_delay=str(delay_us), commit_siblings="2"),
                job_count=job_count, worker_count=worker_count, poll_interval=poll_interval,
            )
        change = (rate / baseline - 1) * 100
        print(f"  {delay_us:>10}us   {rate:>8.0f}   {change:>+6.0f}%")


def run_baseline_suite(conn, *, job_count: int) -> None:
    raw = measure_raw_ceiling(conn, job_count=job_count)
    unbatched = measure_single_process_library(conn, job_count=job_count, batch_size=1)
    batched = measure_single_process_library(conn, job_count=job_count, batch_size=10)

    print()
    print("  single process, same workload")
    print(f"  raw Postgres, one at a time   {raw:>8.0f} jobs/sec")
    print(f"  durable-queue, batch of 1     {unbatched:>8.0f} jobs/sec   "
          f"({(unbatched / raw - 1) * 100:+.0f}% vs raw)")
    print(f"  durable-queue, batch of 10    {batched:>8.0f} jobs/sec   "
          f"({(batched / raw - 1) * 100:+.0f}% vs raw)")


def run_scaling_suite(conn, *, job_count: int, poll_interval: float) -> None:
    print()
    print("  workers   jobs/sec   vs 1 worker   efficiency")
    single = None
    for workers in (1, 2, 4, 8):
        rate = measure_throughput(
            conn, job_count=job_count, worker_count=workers, poll_interval=poll_interval
        )
        if single is None:
            single = rate
        speedup = rate / single
        print(f"  {workers:>7}   {rate:>8.0f}   {speedup:>10.2f}x   {speedup / workers * 100:>9.0f}%")


def run_durability_suite(conn, *, job_count: int, poll_interval: float) -> None:
    """
    What fsync-per-commit costs, at increasing jobs in flight.

    Three settings per row. Everything durable is the default. Workers
    relaxed is run_worker(durable_bookkeeping=False): a worker's own
    claim and completion commits stop waiting for the flush, but the
    enqueue - the caller's commit - stays durable. Losing a worker
    commit in a crash is a re-run, the at-least-once path this queue
    already implements; losing an enqueue would be data loss. Nothing
    durable relaxes the enqueue too, which is roughly where a
    Redis-backed queue sits by default.

    Cost is everything-durable against workers-relaxed: the price of the
    one setting a user actually chooses between.
    """
    print()
    print(f"  {'in flight':>10} {'split':>9} {'durable':>9} {'workers relaxed':>16} "
          f"{'nothing durable':>16} {'cost':>6}")
    for workers, slots in ((1, 4), (2, 8), (4, 8), (8, 8)):
        options = {"concurrency": slots, "batch_size": 25}
        jobs = _jobs_for(job_count, workers * slots)
        durable = measure_throughput(
            conn, job_count=jobs, worker_count=workers, poll_interval=poll_interval, **options,
        )
        worker_relaxed = measure_throughput(
            conn, job_count=jobs, worker_count=workers, poll_interval=poll_interval,
            dsn=_relaxed_dsn(), **options,
        )
        nothing_durable = _measure_with_dsn(
            _relaxed_dsn(), job_count=jobs, worker_count=workers,
            poll_interval=poll_interval, **options,
        )
        cost = (1 - durable / worker_relaxed) * 100
        print(f"  {workers * slots:>10} {f'{workers}w x {slots}s':>9} {durable:>9.0f} "
              f"{worker_relaxed:>16.0f} {nothing_durable:>16.0f} {cost:>5.0f}%")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-notify", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--concurrency", type=int, default=1, help=argparse.SUPPRESS)
    parser.add_argument("--batch-size", type=int, default=10, help=argparse.SUPPRESS)
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--latency-samples", type=int, default=15)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--repeat", type=int, default=1,
                        help="report each throughput figure as the median of this many runs")
    parser.add_argument(
        "--suite",
        choices=("main", "baseline", "scaling", "concurrency", "curve",
                 "durability", "commitdelay", "comparison", "connections", "all"),
        default="main",
    )
    args = parser.parse_args()
    global REPEAT
    REPEAT = args.repeat

    if args.worker:
        run_worker(
            poll_interval=args.poll_interval,
            use_notify=not args.no_notify,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
        )
        return

    if args.suite != "main":
        conn = get_connection()
        try:
            if args.suite in ("baseline", "all"):
                print("baseline: raw Postgres vs durable-queue, single process...")
                run_baseline_suite(conn, job_count=max(args.jobs // 2, 100))
            if args.suite in ("scaling", "all"):
                print("scaling: 1/2/4/8 workers...")
                run_scaling_suite(
                    conn, job_count=args.jobs, poll_interval=args.poll_interval
                )
            if args.suite in ("comparison", "all"):
                print("comparison: naive vs tuned worker configuration...")
                run_comparison_suite(
                    conn, job_count=args.jobs, poll_interval=args.poll_interval
                )
            if args.suite in ("curve", "all"):
                print("curve: throughput vs total jobs in flight...")
                run_curve_suite(
                    conn, job_count=args.jobs, poll_interval=args.poll_interval
                )
            if args.suite in ("connections", "all"):
                print("connections: peak connections held, IO-bound vs transactional...")
                run_connections_suite(
                    conn, job_count=args.jobs, poll_interval=args.poll_interval
                )
            if args.suite in ("concurrency", "all"):
                print("concurrency: slots per worker process...")
                run_concurrency_suite(
                    conn, job_count=args.jobs, worker_count=args.workers,
                    poll_interval=args.poll_interval,
                )
            if args.suite in ("durability", "all"):
                print("durability: synchronous_commit on vs off...")
                run_durability_suite(
                    conn, job_count=args.jobs, poll_interval=args.poll_interval,
                )
            if args.suite in ("commitdelay", "all"):
                print("commit_delay: group-commit tuning, durability unchanged...")
                run_commit_delay_suite(
                    conn, job_count=args.jobs, worker_count=args.workers,
                    poll_interval=args.poll_interval,
                )
        finally:
            conn.close()
        return

    conn = get_connection()
    try:
        print(f"throughput: {args.jobs} jobs, {args.workers} workers...")
        rate = measure_throughput(
            conn, job_count=args.jobs, worker_count=args.workers,
            poll_interval=args.poll_interval,
        )

        print(f"latency: {args.latency_samples} samples, poll interval {args.poll_interval}s...")
        with_notify = measure_latency(
            conn, samples=args.latency_samples, worker_count=args.workers,
            poll_interval=args.poll_interval, use_notify=True,
        )
        without_notify = measure_latency(
            conn, samples=args.latency_samples, worker_count=args.workers,
            poll_interval=args.poll_interval, use_notify=False,
        )
    finally:
        conn.close()

    print()
    print(f"  throughput            {rate:>10.0f} jobs/sec  ({args.workers} workers)")
    print()
    print("  enqueue -> start       LISTEN/NOTIFY      polling only")
    for label, pct in (("p50", 0.50), ("p99", 0.99)):
        notify_value = _percentile(with_notify, pct)
        poll_value = _percentile(without_notify, pct)
        print(f"  {label}              {notify_value:>12.1f} ms {poll_value:>15.1f} ms")
    speedup = _percentile(without_notify, 0.50) / max(_percentile(with_notify, 0.50), 0.001)
    print()
    print(f"  NOTIFY is {speedup:.0f}x faster to start an idle-queue job")


if __name__ == "__main__":
    main()
