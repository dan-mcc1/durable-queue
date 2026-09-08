from durable_queue.db import get_connection
from durable_queue.effects import has_effect, record_effect
from durable_queue.jobs import enqueue
from durable_queue.registry import task
from durable_queue.worker import process_one

_execution_count = {"n": 0}


@task
def idempotent_side_effecting_task(effect_key: str) -> None:
    """
    Stands in for something like "send an email": the side effect
    itself (_execution_count going up) isn't a database write, so it
    can't be protected by a transaction - only the effects ledger
    guards it here.
    """
    effect_conn = get_connection()
    try:
        if has_effect(effect_conn, effect_key):
            return
        _execution_count["n"] += 1
        record_effect(effect_conn, effect_key)
    finally:
        effect_conn.close()


def test_has_effect_is_false_for_an_unrecorded_key(conn):
    assert has_effect(conn, "unrecorded-key") is False


def test_record_effect_then_has_effect_is_true(conn):
    record_effect(conn, "digest:2026-09-08:user_abc")

    assert has_effect(conn, "digest:2026-09-08:user_abc") is True


def test_record_effect_is_safe_to_call_twice(conn):
    record_effect(conn, "some-key")
    record_effect(conn, "some-key")

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM effects WHERE key = %s", ("some-key",))
        assert cur.fetchone()["n"] == 1


def test_effects_ledger_prevents_a_duplicate_effect_across_two_job_rows(conn, worker_id):
    """
    Two separate enqueued jobs (not a retry of the same job - two
    different rows) both point at the same real-world effect. This is
    the scenario a jobs-level idempotency_key can't catch, since it's
    two distinct job ids - only the effects ledger, keyed on the effect
    itself, catches it.
    """
    _execution_count["n"] = 0
    enqueue(conn, "idempotent_side_effecting_task", {"effect_key": "shared-effect"})
    enqueue(conn, "idempotent_side_effecting_task", {"effect_key": "shared-effect"})
    conn.commit()

    process_one(conn, worker_id)
    process_one(conn, worker_id)

    assert _execution_count["n"] == 1
