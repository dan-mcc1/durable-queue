"""
Enqueue through a SQLAlchemy Session, for an app whose ORM runs on a
driver other than psycopg 3 - psycopg2, typically.

The job commits in the session's own transaction, with the ORM writes
it belongs to. That is the reason to enqueue here rather than over a
connection of the queue's own, which would be a second transaction: the
job could commit while the writes it was about rolled back, or the
other way round. Workers still need psycopg 3; this covers only the
enqueueing side.

Needs SQLAlchemy 2.0: `pip install durable-queue[sqlalchemy]`.
"""
import json

from sqlalchemy.orm import Session

from durable_queue.jobs import ENQUEUE_MANY_SQL, ENQUEUE_SQL, JOB_NOTIFY_CHANNEL


def enqueue(
        session: Session,
        task: str,
        args: dict,
        *,
        idempotency_key: str | None = None) -> int:
    """
    durable_queue.jobs.enqueue for a Session. Returns the job's id, or
    the existing job's if idempotency_key matched one.

    Nothing is committed here. session.commit() commits the job along
    with everything else, a rollback discards it, and no worker is woken
    until that commit.

    The session isn't flushed first. If it has autoflush off and the
    args need an id the database hasn't assigned yet, flush before
    calling this.
    """
    # exec_driver_sql passes the SQL to the driver untouched, so the
    # statement can be shared with jobs.enqueue verbatim. text() would
    # parse it for :name parameters, and it misreads ::jsonb.
    result = session.connection().exec_driver_sql(
        ENQUEUE_SQL,
        {
            "task": task,
            "args": json.dumps(args),
            "idempotency_key": idempotency_key,
            "channel": JOB_NOTIFY_CHANNEL,
        },
    )
    return result.scalar_one()


def enqueue_many(
        session: Session,
        task: str,
        args_list: list[dict],
        *,
        idempotency_keys: list[str | None] | None = None) -> list[int]:
    """
    durable_queue.jobs.enqueue_many for a Session: one statement and one
    wakeup, for fan-out. Commits the same way enqueue above does.
    """
    if not args_list:
        return []
    if idempotency_keys is None:
        idempotency_keys = [None] * len(args_list)

    result = session.connection().exec_driver_sql(
        ENQUEUE_MANY_SQL,
        {
            "task": task,
            "args": [json.dumps(a) for a in args_list],
            "idempotency_keys": list(idempotency_keys),
            "channel": JOB_NOTIFY_CHANNEL,
        },
    )
    return list(result.scalars())
