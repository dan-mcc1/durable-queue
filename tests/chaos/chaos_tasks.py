"""
Task definitions used only by the chaos test. Kept in their own module,
separate from the test file, so a spawned worker subprocess can import
just this (registering the @task) without importing pytest or the test
itself.
"""
import random
import time

from durable_queue.db import get_connection
from durable_queue.effects import has_effect, record_effect
from durable_queue.registry import task


@task
def chaos_task(effect_key: str) -> None:
    """
    Stands in for an irreversible external action (send an email, charge
    a card). The observable "did this really happen" signal is a plain
    INSERT into chaos_observations, deliberately not deduped the way
    effects/idempotency_key are - it's the thing the test counts to
    prove the ledger's check-then-act-then-record pattern actually
    prevented a duplicate, not just that it claims to.
    """
    conn = get_connection()
    try:
        if has_effect(conn, effect_key):
            return

        with conn.cursor() as cur:
            cur.execute("INSERT INTO chaos_observations (effect_key) VALUES (%s)", (effect_key,))
        conn.commit()

        # Widens the window a randomly-timed kill can land in, between
        # the effect happening and it being recorded as done - the gap
        # no ledger can close when the effect is external.
        time.sleep(random.uniform(0.05, 0.2))

        record_effect(conn, effect_key)
    finally:
        conn.close()


@task
def sleep_task(seconds: float) -> None:
    """
    Blocks without touching the database, so wall-clock time across a
    batch reveals whether a worker's slots actually overlap or merely
    take turns.
    """
    time.sleep(seconds)


@task
def transactional_chaos_task(conn, effect_key: str) -> None:
    """
    The transactional counterpart to chaos_task, run against the same
    kills - and note what isn't here. No has_effect check, no
    record_effect, no ledger at all.

    Declaring `conn` makes the worker run this inside its transaction,
    so this INSERT commits in the same breath as the job's completion.
    A kill mid-task rolls the INSERT back and the retry redoes it
    cleanly; a kill after the commit leaves the job already finished.
    There is no window where the effect happened but wasn't recorded,
    which is the entire reason chaos_task needs a ledger and this
    doesn't.
    """
    with conn.cursor() as cur:
        cur.execute("INSERT INTO chaos_observations (effect_key) VALUES (%s)", (effect_key,))

    # Widen the window a kill can land in - here it lands mid
    # transaction, which is precisely what makes it harmless.
    time.sleep(random.uniform(0.05, 0.2))
