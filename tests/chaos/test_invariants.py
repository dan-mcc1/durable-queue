"""
M8: chaos testing. Real worker subprocesses, killed with no chance to
clean up (SIGKILL on POSIX; Popen.kill() maps to TerminateProcess on
Windows, which is the equivalent no-cleanup-opportunity primitive on a
platform that has no real SIGKILL), while jobs are actually in flight.

This tests a property across many jobs and many kills - "nothing is
ever lost or duplicated, no matter when a worker dies" - rather than a
single hand-picked example. That's the difference between this and
every earlier test in the suite.
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
    raise AssertionError(f"chaos run did not reach a terminal state for every job within {timeout_seconds}s")


def test_no_job_lost_or_duplicated_when_workers_are_killed_mid_job(conn):
    job_count = 24
    worker_count = 3
    chaos_duration_seconds = 4.0

    effect_keys = [f"chaos-{i}" for i in range(job_count)]
    for key in effect_keys:
        enqueue(conn, "chaos_task", {"effect_key": key})
    conn.commit()

    workers = [_spawn_worker() for _ in range(worker_count)]

    try:
        deadline = time.monotonic() + chaos_duration_seconds
        while time.monotonic() < deadline:
            time.sleep(random.uniform(0.2, 0.4))
            victim = random.choice(workers)
            if victim.poll() is None:  # still alive
                victim.kill()  # no cleanup opportunity - the entire point
                victim.wait()
                workers.remove(victim)
                workers.append(_spawn_worker())

        # Chaos phase over: let whoever's left, plus reaping, finish
        # draining the queue undisturbed.
        _wait_until_all_jobs_are_terminal(conn, timeout_seconds=20.0)
    finally:
        for w in workers:
            w.kill()
        for w in workers:
            w.wait()

    with conn.cursor() as cur:
        cur.execute("SELECT status, count(*) AS n FROM jobs GROUP BY status")
        final_counts = {row["status"]: row["n"] for row in cur.fetchall()}

    # A killed job never consumes an attempt (mark_failed is only ever
    # called by a worker that's still alive), so every job should reach
    # 'succeeded' - none should ever be forced into 'dead'.
    assert final_counts.get("succeeded", 0) == job_count
    assert final_counts.get("dead", 0) == 0

    # The effects LEDGER itself must never have a duplicate key - that's
    # enforced by a Postgres UNIQUE constraint (effects.key PRIMARY KEY)
    # and should hold no matter what chaos happens.
    with conn.cursor() as cur:
        cur.execute("SELECT key, count(*) AS n FROM effects GROUP BY key HAVING count(*) > 1")
        duplicate_ledger_keys = cur.fetchall()
    assert not duplicate_ledger_keys, f"effects ledger has duplicate keys: {duplicate_ledger_keys}"

    # chaos_observations is deliberately NOT deduped - it stands in for
    # the real-world action itself (an email send, a charge), which
    # can't share a transaction with the ledger write. Nothing may be
    # LOST (every job that succeeded must have actually run its effect
    # at least once); a SMALL number of duplicates is the expected,
    # documented cost of the narrow window between "the effect
    # happened" and "record_effect() committed" - proof of exactly the
    # tradeoff DESIGN.md describes, not a bug. What would be a bug is
    # duplicates on most/all jobs, which would mean the has_effect()
    # check isn't doing anything.
    with conn.cursor() as cur:
        cur.execute("SELECT effect_key, count(*) AS n FROM chaos_observations GROUP BY effect_key")
        observation_counts = {row["effect_key"]: row["n"] for row in cur.fetchall()}

    missing = [k for k in effect_keys if observation_counts.get(k, 0) == 0]
    duplicated = [k for k, n in observation_counts.items() if n > 1]

    # If effects go missing, the first thing worth knowing is whether
    # chaos_task skipped them because the ledger already claimed they
    # were done - which would point at leaked state rather than at lost
    # work. Cheap to collect, and it turns a mystified rerun into an
    # answer.
    if missing:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM effects")
            ledger_rows = cur.fetchone()["n"]
        raise AssertionError(
            f"effects never observed despite the job succeeding: {missing}\n"
            f"effects ledger holds {ledger_rows} rows; if that covers the missing "
            f"keys, has_effect() short-circuited them and the ledger was polluted "
            f"before the run rather than the work being lost"
        )
    print(
        f"\nchaos result: {len(duplicated)}/{job_count} effects duplicated "
        f"(expected: small but possibly nonzero - the narrow at-least-once window)"
    )
    assert len(duplicated) < job_count // 2, (
        f"far too many duplicates ({duplicated}) - the has_effect() check "
        "doesn't appear to be preventing anything"
    )
