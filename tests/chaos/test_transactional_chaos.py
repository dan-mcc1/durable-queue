"""
The A/B counterpart to test_invariants.py. Identical chaos - real
worker subprocesses killed mid-job at random - against a task that
runs inside the worker's transaction instead of guarding itself with
an effects ledger.

test_invariants.py reliably produces a handful of duplicated effects,
because its task does something external-shaped that can't share a
transaction with the ledger write. This one must produce exactly zero,
with no idempotency machinery whatsoever, because the effect and the
job's completion commit together. Same harness, same kills, different
guarantee - and the difference is entirely down to whether the side
effect lives inside Postgres.
"""
import random
import subprocess
import sys
import time
from pathlib import Path

from durable_queue.jobs import enqueue

REPO_ROOT = Path(__file__).resolve().parents[2]


def _spawn_worker() -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "tests.chaos.chaos_worker"],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_until_all_jobs_are_terminal(conn, *, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM jobs WHERE status NOT IN ('succeeded', 'dead')")
            remaining = cur.fetchone()["n"]
        if remaining == 0:
            return
        time.sleep(0.2)
    raise AssertionError(f"chaos run did not finish within {timeout_seconds}s")


def test_transactional_task_is_exactly_once_under_worker_kills(conn):
    job_count = 24
    worker_count = 3
    chaos_duration_seconds = 4.0

    effect_keys = [f"txn-chaos-{i}" for i in range(job_count)]
    for key in effect_keys:
        enqueue(conn, "transactional_chaos_task", {"effect_key": key})
    conn.commit()

    workers = [_spawn_worker() for _ in range(worker_count)]
    try:
        deadline = time.monotonic() + chaos_duration_seconds
        while time.monotonic() < deadline:
            time.sleep(random.uniform(0.2, 0.4))
            victim = random.choice(workers)
            if victim.poll() is None:
                victim.kill()
                victim.wait()
                workers.remove(victim)
                workers.append(_spawn_worker())

        _wait_until_all_jobs_are_terminal(conn, timeout_seconds=20.0)
    finally:
        for w in workers:
            w.kill()
        for w in workers:
            w.wait()

    with conn.cursor() as cur:
        cur.execute("SELECT status, count(*) AS n FROM jobs GROUP BY status")
        final_counts = {row["status"]: row["n"] for row in cur.fetchall()}
    assert final_counts.get("succeeded", 0) == job_count
    assert final_counts.get("dead", 0) == 0

    with conn.cursor() as cur:
        cur.execute("SELECT effect_key, count(*) AS n FROM chaos_observations GROUP BY effect_key")
        counts = {row["effect_key"]: row["n"] for row in cur.fetchall()}

    missing = [k for k in effect_keys if counts.get(k, 0) == 0]
    duplicated = [k for k, n in counts.items() if n > 1]

    assert not missing, f"effects lost despite the job succeeding: {missing}"
    # Exactly zero, not "a small number" - this is the whole point.
    assert not duplicated, f"transactional task still duplicated effects: {duplicated}"
