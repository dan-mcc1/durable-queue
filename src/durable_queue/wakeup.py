"""
Waking idle workers and schedulers in this process.

In idle mode a worker or scheduler sleeps until its next known due time
without sending a query, which is what lets a database that scales to
zero (Neon) go to sleep too. The price is that it can't hear about new
work through the database while it sleeps. So whatever creates work in
this process - a schedule firing, an enqueue committing, a job failing
and leaving a retry - calls wake(), and every idle loop here takes a
look.
"""
import threading
from time import sleep

# Before a sleep longer than this, an idle loop closes its connections
# rather than leave them for the database to close while it's asleep.
CLOSE_CONNECTIONS_AFTER = 60.0
# The least an idle loop sleeps, so something due but not yet claimable
# - a job another worker has locked - can't turn it into a busy loop.
MIN_SLEEP = 0.1

_changed = threading.Condition()
_generation = 0


def current() -> int:
    """Read before checking for work, and pass to wait() afterwards."""
    with _changed:
        return _generation


def wake() -> None:
    global _generation
    with _changed:
        _generation += 1
        _changed.notify_all()


def wait(seen: int, timeout: float | None) -> None:
    """
    Sleep until wake() is called, or timeout passes (None: no limit).

    Returns at once if wake() has been called since `seen` was read, so
    work created between checking for it and going to sleep isn't missed
    until the next timeout.
    """
    with _changed:
        _changed.wait_for(lambda: _generation != seen, timeout)


def pause(seconds: float, stop: threading.Event | None) -> None:
    """sleep(), cut short if stop is set."""
    if stop is None:
        sleep(seconds)
    else:
        stop.wait(seconds)
