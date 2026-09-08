"""
Task definitions for the fan-out/combine demo: sum a large range of
integers by splitting it into many independent chunks. Kept in their
own module, separate from the test file, so a spawned worker
subprocess can import just this to register the @tasks.
"""
from durable_queue.db import get_connection
from durable_queue.jobs import enqueue
from durable_queue.registry import task


@task
def solve_chunk(problem_id: int, chunk_index: int, start: int, end: int) -> None:
    """
    Compute one chunk's partial sum and fold it into the running total.

    Recording the chunk's result and counting it toward the total are
    both plain Postgres writes with nothing external in between, so
    they share one transaction (no conn.commit() between the two
    statements below). A kill anywhere before the commit loses both;
    a kill after loses neither. There's no crash window here the way
    there is in chaos_tasks.chaos_task, precisely because nothing
    outside Postgres happens in the middle.

    The chunk_results INSERT is itself the idempotency check: retrying
    this job after a kill re-runs the whole function, but ON CONFLICT
    DO NOTHING means a chunk already recorded (and therefore already
    counted, since those two things can only ever happen together)
    contributes nothing a second time.
    """
    conn = get_connection()
    try:
        partial_sum = sum(range(start, end + 1))

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chunk_results (problem_id, chunk_index, partial_sum)
                VALUES (%s, %s, %s)
                ON CONFLICT (problem_id, chunk_index) DO NOTHING
                RETURNING chunk_index
                """,
                (problem_id, chunk_index, partial_sum),
            )
            newly_recorded = cur.fetchone() is not None

            row = None
            if newly_recorded:
                cur.execute(
                    """
                    UPDATE problems SET completed_chunks = completed_chunks + 1
                    WHERE id = %s
                    RETURNING completed_chunks, total_chunks
                    """,
                    (problem_id,),
                )
                row = cur.fetchone()

        conn.commit()  # both statements land together, or neither does

        if row is not None and row["completed_chunks"] == row["total_chunks"]:
            # Cheap insurance, not the real defense: the atomic
            # increment above already makes it structurally impossible
            # for two chunks to both observe the "last one" condition.
            enqueue(
                conn,
                "combine_problem",
                {"problem_id": problem_id},
                idempotency_key=f"combine:{problem_id}",
            )
            conn.commit()
    finally:
        conn.close()


@task
def combine_problem(problem_id: int) -> None:
    """
    Sum every recorded chunk and mark the problem solved. Safe to run
    more than once - it's a pure recomputation from chunk_results with
    no counters or external effects involved.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sum(partial_sum) AS total FROM chunk_results WHERE problem_id = %s",
                (problem_id,),
            )
            total = cur.fetchone()["total"]
            cur.execute(
                "UPDATE problems SET status = 'solved', result = %s WHERE id = %s",
                (total, problem_id),
            )
        conn.commit()
    finally:
        conn.close()
