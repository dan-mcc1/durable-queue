import os
from collections.abc import Callable

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

# Backoff between attempts when a long-running loop loses its connection.
RECONNECT_MIN_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0

# Applied wherever the DSN doesn't set its own. Without keepalives, a
# connection whose other end vanished without closing it - a NAT
# timeout, a host that lost power - looks healthy until the kernel gives
# up retransmitting, ~15 minutes on Linux, and a worker blocked on it
# does nothing for that long. Probes aren't queries, so they don't keep
# a database that scales to zero awake.
_CONNECTION_DEFAULTS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
    # The case keepalives miss: a query sent into a dead connection,
    # where unacknowledged data stops probes being sent at all.
    "tcp_user_timeout": 60_000,
    # So the queue's sessions can be told apart in pg_stat_activity.
    "application_name": "durable-queue",
}


def get_dsn() -> str:
    """
    DURABLE_QUEUE_DATABASE_URL if set, otherwise DATABASE_URL.

    The separate variable is for an app whose DATABASE_URL goes through
    a transaction-mode pooler such as PgBouncer (Neon's -pooler endpoint
    is one). LISTEN can't work through one - the pooler hands the
    session that ran LISTEN on to other clients - and nothing errors:
    the worker never hears an enqueue and quietly falls back to polling.
    Keep the app on the pooler and point this at the direct endpoint.
    """
    return os.environ.get("DURABLE_QUEUE_DATABASE_URL") or os.environ.get(
        "DATABASE_URL",
        "postgres://durable_queue:durable_queue@localhost:5432/durable_queue_dev",
    )


def connection_kwargs(dsn: str) -> dict:
    """The connection defaults that the DSN doesn't already set."""
    given = conninfo_to_dict(dsn)
    return {key: value for key, value in _CONNECTION_DEFAULTS.items() if key not in given}


def get_connection(dsn: str | None = None) -> psycopg.Connection:
    dsn = dsn or get_dsn()
    return psycopg.connect(dsn, row_factory=dict_row, **connection_kwargs(dsn))


class ReconnectingConnection:
    """
    A connection that is reopened on its next use after it breaks.

    psycopg never reconnects by itself, and some hosts drop connections
    as routine: Neon restarts its computes about weekly to apply
    updates, and closes every connection when it scales to zero. Before
    this, a worker's claim connection dropping ended the process, and
    its heartbeat connection dropping silently ended the heartbeat.

    Reopened lazily, on next use rather than as soon as the break is
    noticed, because on a database that scales to zero, connecting is
    what wakes it. Reconnecting eagerly would keep it awake for good.

    setup replays the session state a fresh connection lacks: LISTEN,
    session settings, autocommit.
    """

    def __init__(
            self,
            dsn: str | None = None,
            setup: Callable[[psycopg.Connection], None] | None = None) -> None:
        self._dsn = dsn
        self._setup = setup
        self._conn: psycopg.Connection | None = None

    def get(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            conn = get_connection(self._dsn)
            try:
                if self._setup is not None:
                    self._setup(conn)
            except BaseException:
                conn.close()
                raise
            self._conn = conn
        return self._conn

    def close(self) -> None:
        """Close the connection, after an error say. The next get() opens a fresh one."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
