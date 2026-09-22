"""
A concrete "large math problem split into sub-problems" test: sum
every integer from 1 to N by fanning out into many independent chunk
jobs, each solved by whichever worker gets to it, with the last chunk
to finish triggering a combine step that produces the final answer.
Run under the same kind of chaos as test_invariants.py (workers killed
at random with no chance to clean up), reusing the same harness shape.

The result asserted here is exact, not "small number of duplicates
expected" the way the effects-ledger chaos task's is - see
math_tasks.py: recording a chunk and counting it share one transaction
with no external call in between, so a kill can't split them apart
the way it can with something like an email send.
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
        [sys.executable, "-m", "tests.chaos.math_worker"],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for_problem_solved(conn, problem_id: int, *, timeout_seconds: float) -> int:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute("SELECT status, result FROM problems WHERE id = %s", (problem_id,))
            row = cur.fetchone()
        if row["status"] == "solved":
            return row["result"]
        time.sleep(0.2)
    raise AssertionError(f"problem {problem_id} was not solved within {timeout_seconds}s")


def test_sum_of_a_range_is_exactly_correct_despite_workers_being_killed(conn):
    n = 100_000
    chunk_size = 2_500
    chunk_starts = list(range(1, n + 1, chunk_size))
    total_chunks = len(chunk_starts)
    worker_count = 3
    chaos_duration_seconds = 4.0

    with conn.cursor() as cur:
        cur.execute("INSERT INTO problems (total_chunks) VALUES (%s) RETURNING id", (total_chunks,))
        problem_id = cur.fetchone()["id"]

    for chunk_index, start in enumerate(chunk_starts):
        end = min(start + chunk_size - 1, n)
        enqueue(
            conn,
            "solve_chunk",
            {"problem_id": problem_id, "chunk_index": chunk_index, "start": start, "end": end},
            idempotency_key=f"chunk:{problem_id}:{chunk_index}",
        )
    conn.commit()

    workers = [_spawn_worker() for _ in range(worker_count)]
    try:
        deadline = time.monotonic() + chaos_duration_seconds
        while time.monotonic() < deadline:
            time.sleep(random.uniform(0.2, 0.4))
            victim = random.choice(workers)
            if victim.poll() is None:  # still alive
                victim.kill()  # no cleanup opportunity
                victim.wait()
                workers.remove(victim)
                workers.append(_spawn_worker())

        result = _wait_for_problem_solved(conn, problem_id, timeout_seconds=20.0)
    finally:
        for w in workers:
            w.kill()
        for w in workers:
            w.wait()

    expected = n * (n + 1) // 2
    assert result == expected

    # Exact, not bounded: every chunk must have recorded its result
    # precisely once, because the record+count step is genuinely atomic.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM chunk_results WHERE problem_id = %s", (problem_id,))
        assert cur.fetchone()["n"] == total_chunks
