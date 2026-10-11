"""
Running the queue inside an app's own process - a web backend, say -
rather than as a service of its own.
"""
import logging
import threading
from collections.abc import Callable

from durable_queue import wakeup
from durable_queue.db import get_connection
from durable_queue.scheduler import run_scheduler
from durable_queue.worker import run_worker

logger = logging.getLogger(__name__)

_RESTART_DELAY = 5.0


class Runner:
    """
    A worker and a scheduler on background threads, both in idle mode:
    with nothing due they send no queries and hold no connections, so a
    database that scales to zero (Neon) can, and they wake when a
    schedule or a retry comes due or this process enqueues through
    durable_queue.sqlalchemy. See run_worker's idle for what does and
    doesn't wake them.

    From a FastAPI lifespan:

        runner = Runner(concurrency=4)

        @asynccontextmanager
        async def lifespan(app):
            runner.start()
            yield
            await asyncio.to_thread(runner.stop)

    One per deployment is enough. More are safe - jobs and schedules are
    claimed under row locks - but each wakes only for enqueues in its
    own process.

    worker_options are passed to run_worker: lease_seconds,
    batch_size, shutdown_timeout and so on.
    """

    def __init__(self, *, concurrency: int = 1, dsn: str | None = None, **worker_options) -> None:
        self._dsn = dsn
        self._worker_options = {"concurrency": concurrency, "dsn": dsn, **worker_options}
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        # Fail now, in the app's startup, if the database can't be
        # reached - not later on a background thread nobody is watching.
        get_connection(self._dsn).close()
        loops = (
            ("durable-queue-worker", run_worker, self._worker_options),
            ("durable-queue-scheduler", run_scheduler, {"dsn": self._dsn}),
        )
        for name, loop, options in loops:
            thread = threading.Thread(
                target=self._keep_running,
                args=(loop, {**options, "idle": True, "stop": self._stop}),
                name=name,
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def stop(self, timeout: float | None = None) -> None:
        """
        Stop both, letting running jobs finish for up to the worker's
        shutdown_timeout. Blocks, so call it from async code through
        asyncio.to_thread.
        """
        self._stop.set()
        wakeup.wake()
        for thread in self._threads:
            thread.join(timeout)

    def _keep_running(self, loop: Callable[..., None], options: dict) -> None:
        # Losing the database is handled inside the loops; this is for
        # anything else. A loop that died here would leave the app up
        # and serving with nothing running its jobs, and nothing saying
        # so.
        while not self._stop.is_set():
            try:
                loop(**options)
            except Exception:
                logger.exception(
                    "%s crashed, restarting in %.0fs",
                    threading.current_thread().name, _RESTART_DELAY,
                )
                self._stop.wait(_RESTART_DELAY)
