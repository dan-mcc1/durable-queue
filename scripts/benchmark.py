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
import subprocess
import sys
import time

from urllib.parse import quote

import psycopg
from psycopg.rows import dict_row

from durable_queue.db import get_connection
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


def _reset(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE jobs, bench_samples")
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


def _drain(conn, *, timeout_seconds: float = 120.0) -> float:
    """Wait for the queue to empty, returning elapsed seconds."""
    started = time.monotonic()
    deadline = started + timeout_seconds
    while True:
        if not _any_unfinished(conn):
            return time.monotonic() - started
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"queue did not drain within {timeout_seconds}s; "
                f"{_pending_count(conn)} jobs left"
            )
        time.sleep(0.05)


def measure_throughput(conn, *, job_count: int, worker_count: int, poll_interval: float,
                       dsn: str | None = None, concurrency: int = 1,
                       batch_size: int = 10, use_notify: bool = True) -> float:
    _reset(conn)
    workers = _spawn_workers(
        worker_count, use_notify=use_notify, poll_interval=poll_interval, dsn=dsn,
        concurrency=concurrency, batch_size=batch_size,
    )
    try:
        now = time.time()
        enqueue_many(conn, "bench_job", [{"enqueued_at": now}] * job_count)
        conn.commit()
        elapsed = _drain(conn)
    finally:
        _stop(workers)
    return job_count / elapsed


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
    base = os.environ.get(
        "DATABASE_URL",
        "postgres://durable_queue:durable_queue@localhost:5432/durable_queue_dev",
    )
    options = quote(" ".join(f"-c {key}={value}" for key, value in settings.items()), safe="")
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}options={options}"


def _relaxed_dsn() -> str:
    return _dsn_with(synchronous_commit="off")


def _measure_with_dsn(dsn: str, *, job_count: int, worker_count: int,
                      poll_interval: float) -> float:
    tuned_conn = psycopg.connect(dsn, row_factory=dict_row)
    try:
        return measure_throughput(
            tuned_conn, job_count=job_count, worker_count=worker_count,
            poll_interval=poll_interval, dsn=dsn,
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
            conn, job_count=job_count, worker_count=workers,
            poll_interval=poll_interval, **NAIVE,
        )
        tuned = measure_throughput(
            conn, job_count=job_count * 5, worker_count=workers,
            poll_interval=poll_interval, **TUNED,
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
    """
    print()
    print(f"  {'in flight':>10} {'split':>10} {'jobs/sec':>10} {'marginal':>10}")
    previous = None
    for workers, slots in ((1, 1), (1, 2), (1, 4), (1, 8), (2, 8), (4, 8), (8, 8)):
        rate = measure_throughput(
            conn, job_count=job_count, worker_count=workers,
            poll_interval=poll_interval, concurrency=slots, batch_size=25,
        )
        marginal = "-" if previous is None else f"{rate / previous:.2f}x"
        split = f"{workers}w x {slots}s"
        print(f"  {workers * slots:>10} {split:>10} {rate:>10.0f} {marginal:>10}")
        previous = rate


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


def run_durability_suite(conn, *, job_count: int, worker_count: int, poll_interval: float) -> None:
    print("  ... synchronous_commit = on", flush=True)
    durable = measure_throughput(
        conn, job_count=job_count, worker_count=worker_count, poll_interval=poll_interval
    )

    # Relaxing fsync-per-commit puts Postgres roughly where a Redis-backed
    # queue sits by default - the apples-to-apples setting for any
    # cross-system comparison.
    # Workers relaxed, enqueues still fully durable. Losing a worker's
    # claim or completion commit in a crash is not data loss - it's a
    # re-run, which is the at-least-once path this queue already
    # implements. Losing an *enqueue* would be data loss, and that
    # commit belongs to the caller's transaction, which stays durable.
    print("  ... workers relaxed, enqueues durable", flush=True)
    worker_relaxed = measure_throughput(
        conn, job_count=job_count, worker_count=worker_count,
        poll_interval=poll_interval, dsn=_relaxed_dsn(),
    )

    print("  ... synchronous_commit = off", flush=True)
    relaxed = _measure_with_dsn(
        _relaxed_dsn(), job_count=job_count, worker_count=worker_count,
        poll_interval=poll_interval,
    )

    print()
    print(f"  everything durable            {durable:>8.0f} jobs/sec   (every commit fsynced)")
    print(f"  workers relaxed               {worker_relaxed:>8.0f} jobs/sec   (enqueues still durable; "
          "a lost worker commit is a re-run)")
    print(f"  nothing durable               {relaxed:>8.0f} jobs/sec   (an enqueue can vanish - real data loss)")
    print()
    print(f"  cost of full durability       {(1 - durable / relaxed) * 100:>8.0f}%")
    print(f"  cost of durable enqueues only {(1 - worker_relaxed / relaxed) * 100:>8.0f}%")


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
    parser.add_argument(
        "--suite",
        choices=("main", "baseline", "scaling", "concurrency", "curve",
                 "durability", "commitdelay", "comparison", "all"),
        default="main",
    )
    args = parser.parse_args()

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
            if args.suite in ("concurrency", "all"):
                print("concurrency: slots per worker process...")
                run_concurrency_suite(
                    conn, job_count=args.jobs, worker_count=args.workers,
                    poll_interval=args.poll_interval,
                )
            if args.suite in ("durability", "all"):
                print("durability: synchronous_commit on vs off...")
                run_durability_suite(
                    conn, job_count=args.jobs, worker_count=args.workers,
                    poll_interval=args.poll_interval,
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
