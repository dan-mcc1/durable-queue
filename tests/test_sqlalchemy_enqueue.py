"""
Enqueueing through a SQLAlchemy Session on psycopg2 - ReleaseRadar's
setup - where the job has to commit in the session's own transaction,
together with the writes it belongs to.
"""
import pytest

pytest.importorskip("sqlalchemy")
pytest.importorskip("psycopg2")

from sqlalchemy import create_engine, make_url, text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from durable_queue import sqlalchemy as dq  # noqa: E402
from durable_queue.db import get_connection, get_dsn  # noqa: E402
from durable_queue.effects import has_effect  # noqa: E402
from durable_queue.jobs import get_job  # noqa: E402
from durable_queue.worker import listen_for_jobs, wait_for_job  # noqa: E402


@pytest.fixture
def session(conn):  # after conn, whose TRUNCATE needs no session holding locks
    engine = create_engine(make_url(get_dsn()).set(drivername="postgresql+psycopg2"))
    with Session(engine) as orm_session:
        yield orm_session
    engine.dispose()


@pytest.fixture
def listener():
    listening = get_connection()
    listen_for_jobs(listening)
    yield listening
    listening.close()


def test_a_job_commits_with_the_sessions_own_writes(conn, session):
    session.execute(text("INSERT INTO effects (key) VALUES ('order-1')"))
    job_id = dq.enqueue(session, "some_task", {"order": "order-1", "note": "it's"})

    assert get_job(conn, job_id) is None  # invisible until the session commits
    session.commit()

    assert get_job(conn, job_id)["args"] == {"order": "order-1", "note": "it's"}
    assert has_effect(conn, "order-1")


def test_a_rollback_takes_the_job_with_it_and_wakes_nobody(conn, session, listener):
    session.execute(text("INSERT INTO effects (key) VALUES ('order-2')"))
    job_id = dq.enqueue(session, "some_task", {})
    session.rollback()

    assert get_job(conn, job_id) is None
    assert not has_effect(conn, "order-2")
    assert wait_for_job(listener, 0.3) is False


def test_the_commit_is_what_wakes_a_worker(session, listener):
    dq.enqueue(session, "some_task", {})
    assert wait_for_job(listener, 0.3) is False

    session.commit()
    assert wait_for_job(listener, 5.0) is True


def test_an_idempotency_key_returns_the_existing_job(session):
    first = dq.enqueue(session, "some_task", {}, idempotency_key="digest:2026-10-10:u1")
    session.commit()
    second = dq.enqueue(session, "some_task", {}, idempotency_key="digest:2026-10-10:u1")
    session.commit()

    assert second == first


def test_enqueue_many_fans_out_in_one_statement(conn, session):
    args_list = [{"user": "u1"}, {"user": "u2", "note": "it's"}, {"nested": {"a": [1, 2]}}]
    ids = dq.enqueue_many(session, "some_task", args_list, idempotency_keys=["k1", None, "k3"])
    session.commit()

    jobs = [get_job(conn, job_id) for job_id in sorted(ids)]
    assert [job["args"] for job in jobs] == args_list
    assert [job["idempotency_key"] for job in jobs] == ["k1", None, "k3"]
    assert dq.enqueue_many(session, "some_task", []) == []
