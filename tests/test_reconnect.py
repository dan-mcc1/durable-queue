"""
Connections the server drops - routine on a host like Neon, which
restarts computes to update them and closes every connection when it
scales to zero - are reopened rather than being the end of whatever
held them.
"""
from time import sleep

import psycopg
import pytest

from durable_queue.db import ReconnectingConnection, connection_kwargs, get_dsn
from durable_queue.jobs import claim_jobs, enqueue, get_job
from durable_queue.worker import _Heartbeat, listen_for_jobs, wait_for_job


def _terminate(conn, pid: int) -> None:
    with conn.cursor() as cur:
        # The timeout makes this wait until the backend has exited.
        cur.execute("SELECT pg_terminate_backend(%s, 5000)", (pid,))
    conn.commit()


def test_a_dropped_connection_is_reopened_with_its_session_state(conn):
    """
    LISTEN lives and dies with its session, so a reopened connection
    is only any use to a worker if setup has replayed it.
    """
    reconnecting = ReconnectingConnection(setup=listen_for_jobs)
    try:
        dropped = reconnecting.get()
        _terminate(conn, dropped.info.backend_pid)
        with pytest.raises(psycopg.OperationalError):
            dropped.execute("SELECT 1")

        reopened = reconnecting.get()
        assert reopened is not dropped

        enqueue(conn, "some_task", {})
        conn.commit()
        assert wait_for_job(reopened, 5.0) is True
    finally:
        reconnecting.close()


def test_the_heartbeat_outlives_its_connection(conn, worker_id):
    """
    A heartbeat thread used to end on its first database error, leaving
    the worker running jobs whose leases nobody was extending.
    """
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_jobs(conn, worker_id, lease_seconds=60, batch_size=1)

    heartbeat_conn = ReconnectingConnection()
    _terminate(conn, heartbeat_conn.get().info.backend_pid)
    heartbeat = _Heartbeat(heartbeat_conn, worker_id, lease_seconds=60, interval=0.05)
    heartbeat.hold([job_id])
    heartbeat.start()
    try:
        sleep(0.3)  # the first beat fails on the dead connection, later ones reconnect
        before = get_job(conn, job_id)["locked_until"]
        sleep(0.3)
        assert get_job(conn, job_id)["locked_until"] > before
    finally:
        heartbeat.stop()
        heartbeat_conn.close()


def test_the_queues_own_url_wins_over_the_apps(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://app@db-pooler.example/db")
    monkeypatch.setenv("DURABLE_QUEUE_DATABASE_URL", "postgresql://worker@db.example/db")
    assert get_dsn() == "postgresql://worker@db.example/db"

    monkeypatch.delenv("DURABLE_QUEUE_DATABASE_URL")
    assert get_dsn() == "postgresql://app@db-pooler.example/db"


def test_connections_get_keepalives_unless_the_dsn_sets_its_own(conn):
    assert conn.info.get_parameters()["keepalives_idle"] == "30"

    own = connection_kwargs("postgresql://u@db.example/db?keepalives_idle=5")
    assert "keepalives_idle" not in own
    assert own["keepalives"] == 1
