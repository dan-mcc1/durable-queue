import psycopg


def has_effect(conn: psycopg.Connection, key: str) -> bool:
    """Check whether an irreversible effect with this key has already happened."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM effects WHERE key = %s", (key,))
        found = cur.fetchone() is not None
    return found


def record_effect(conn: psycopg.Connection, key: str) -> None:
    """
    Record that an effect happened. Safe to call more than once with the
    same key - a duplicate record is silently ignored, not an error.

    Intended usage inside a task is check-then-act-then-record, in that
    order: has_effect() before doing the irreversible thing, record_effect()
    only after it succeeds. Recording first would close the crash window
    in the wrong direction - a crash between record and act would leave
    the ledger claiming something happened that never did (at-most-once).
    Recording after biases toward at-least-once instead: the effect is
    never silently lost, only (rarely) duplicated, which is why the
    external call it wraps still needs its own stable idempotency key.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO effects (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
            (key,),
        )
    conn.commit()
