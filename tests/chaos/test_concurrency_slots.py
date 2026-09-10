"""
Concurrency slots: one worker process running several jobs at once.

The point isn't that the parameter exists, it's that the jobs genuinely
overlap - so these tests measure wall-clock time against work that is
pure waiting, which is what a realistic task (an HTTP call, an email
send) spends nearly all of its time doing.
"""
import subprocess
import sys
import time
from pathlib import Path

from durable_queue.jobs import enqueue

REPO_ROOT = Path(__file__).resolve().parents[2]
JOB_SECONDS = 0.5
JOB_COUNT = 8


def _spawn_worker(concurrency: int) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "tests.chaos.slots_worker", str(concurrency)],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _drain_seconds(conn, *, concurrency: int, timeout_seconds: float = 60.0) -> float:
    for _ in range(JOB_COUNT):
        enqueue(conn, "sleep_task", {"seconds": JOB_SECONDS})
    conn.commit()

    worker = _spawn_worker(concurrency)
    try:
        started = time.monotonic()
        deadline = started + timeout_seconds
        while True:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) AS n FROM jobs WHERE status NOT IN ('succeeded', 'dead')"
                )
                remaining = cur.fetchone()["n"]
            conn.commit()
            if remaining == 0:
                return time.monotonic() - started
            if time.monotonic() > deadline:
                raise AssertionError(f"{remaining} jobs still unfinished after {timeout_seconds}s")
            time.sleep(0.05)
    finally:
        worker.kill()
        worker.wait()


def test_slots_run_jobs_concurrently_not_merely_in_turn(conn):
    """
    Eight jobs that sleep half a second each. Serially that's 4s of
    unavoidable waiting; with four slots it should be roughly a
    quarter of that. The bound is deliberately loose - this asserts
    overlap happened, not a precise speedup.
    """
    serial_ceiling = JOB_COUNT * JOB_SECONDS  # 4.0s

    elapsed = _drain_seconds(conn, concurrency=4)

    assert elapsed < serial_ceiling / 2, (
        f"eight {JOB_SECONDS}s jobs took {elapsed:.2f}s across 4 slots; "
        f"serial would be ~{serial_ceiling:.1f}s, so they did not overlap"
    )


def test_a_single_slot_still_runs_jobs_one_at_a_time(conn):
    """
    The control for the test above: with one slot the same work must
    take at least the serial time, confirming the speedup came from
    concurrency rather than the jobs not really sleeping.
    """
    elapsed = _drain_seconds(conn, concurrency=1)

    assert elapsed >= JOB_COUNT * JOB_SECONDS * 0.9
