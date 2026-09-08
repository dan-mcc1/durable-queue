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
        # the effect happening and it being recorded as done - the
        # exact gap M5's design doc calls unavoidable.
        time.sleep(random.uniform(0.05, 0.2))

        record_effect(conn, effect_key)
    finally:
        conn.close()
