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
import subprocess
import sys
import time

from durable_queue.db import get_connection
from durable_queue.jobs import enqueue
from durable_queue.registry import task
from durable_queue.worker import run_worker

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


def _spawn_workers(count: int, *, use_notify: bool, poll_interval: float) -> list:
    argv = [sys.executable, __file__, "--worker", "--poll-interval", str(poll_interval)]
    if not use_notify:
        argv.append("--no-notify")
    workers = [
        subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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
        return cur.fetchone()["n"]


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(int(len(ordered) * pct), len(ordered) - 1)
    return ordered[index]


def measure_throughput(conn, *, job_count: int, worker_count: int, poll_interval: float) -> float:
    _reset(conn)
    workers = _spawn_workers(worker_count, use_notify=True, poll_interval=poll_interval)
    try:
        now = time.time()
        for _ in range(job_count):
            enqueue(conn, "bench_job", {"enqueued_at": now})
        conn.commit()

        started = time.monotonic()
        while _pending_count(conn) > 0:
            time.sleep(0.05)
        elapsed = time.monotonic() - started
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

        deadline = time.monotonic() + 30
        while _pending_count(conn) > 0 and time.monotonic() < deadline:
            time.sleep(0.05)

        with conn.cursor() as cur:
            cur.execute("SELECT latency_ms FROM bench_samples")
            return [row["latency_ms"] for row in cur.fetchall()]
    finally:
        _stop(workers)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-notify", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--latency-samples", type=int, default=15)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    args = parser.parse_args()

    if args.worker:
        run_worker(poll_interval=args.poll_interval, use_notify=not args.no_notify)
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
