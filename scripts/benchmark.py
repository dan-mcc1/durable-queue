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
from durable_queue.worker import generate_worker_id, process_one, run_worker

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
                   dsn: str | None = None) -> list:
    argv = [sys.executable, __file__, "--worker", "--poll-interval", str(poll_interval)]
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
        remaining = _pending_count(conn)
        if remaining == 0:
            return time.monotonic() - started
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"queue did not drain within {timeout_seconds}s; {remaining} jobs left"
            )
        time.sleep(0.05)


def measure_throughput(conn, *, job_count: int, worker_count: int, poll_interval: float,
                       dsn: str | None = None) -> float:
    _reset(conn)
    workers = _spawn_workers(
        worker_count, use_notify=True, poll_interval=poll_interval, dsn=dsn
    )
    try:
        now = time.time()
        for _ in range(job_count):
            enqueue(conn, "bench_job", {"enqueued_at": now})
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


def measure_single_process_library(conn, *, job_count: int) -> float:
    """durable-queue draining the same queue, one process, no subprocess noise."""
    _reset(conn)
    now = time.time()
    for _ in range(job_count):
        enqueue(conn, "bench_job", {"enqueued_at": now})
    conn.commit()

    heartbeat_conn = get_connection()
    worker_id = generate_worker_id()
    try:
        started = time.monotonic()
        while process_one(conn, worker_id, heartbeat_conn=heartbeat_conn):
            pass
        elapsed = time.monotonic() - started
    finally:
        heartbeat_conn.close()
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
    library = measure_single_process_library(conn, job_count=job_count)
    overhead = (1 - library / raw) * 100

    print()
    print("  single process, same workload")
    print(f"  raw Postgres claim/complete   {raw:>8.0f} jobs/sec")
    print(f"  durable-queue                 {library:>8.0f} jobs/sec")
    print(f"  library overhead              {overhead:>8.0f}%")


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
    print("  ... synchronous_commit = off", flush=True)
    relaxed = _measure_with_dsn(
        _relaxed_dsn(), job_count=job_count, worker_count=worker_count,
        poll_interval=poll_interval,
    )

    print()
    print(f"  synchronous_commit = on       {durable:>8.0f} jobs/sec   (every commit fsynced)")
    print(f"  synchronous_commit = off      {relaxed:>8.0f} jobs/sec   (commits may be lost on crash)")
    print(f"  cost of durability            {(1 - durable / relaxed) * 100:>8.0f}%")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-notify", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--latency-samples", type=int, default=15)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument(
        "--suite",
        choices=("main", "baseline", "scaling", "durability", "commitdelay", "all"),
        default="main",
    )
    args = parser.parse_args()

    if args.worker:
        run_worker(poll_interval=args.poll_interval, use_notify=not args.no_notify)
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
