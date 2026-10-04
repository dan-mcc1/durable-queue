import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


class PermanentError(Exception):
    """
    Raise from a task to dead-letter the job now instead of retrying it.

    For failures that no amount of waiting fixes - a 404 for a record
    that's been deleted, a payload that fails validation. Retrying those
    only burns attempts and delays the moment someone sees them in
    `durable-queue dead`. Anything else a task raises is assumed
    transient and retried with backoff.
    """


@dataclass(frozen=True)
class RegisteredTask:
    fn: Callable[..., Any]
    wants_connection: bool
    # None means "use the default": the job row's max_attempts, and the
    # worker's max_execution_seconds.
    max_attempts: int | None = None
    max_execution_seconds: float | None = None


_registry: dict[str, RegisteredTask] = {}


def task(
        fn: Callable[..., Any] | None = None,
        *,
        max_attempts: int | None = None,
        max_execution_seconds: float | None = None) -> Any:
    """
    Register a task, as `@task` or `@task(max_attempts=3, ...)`.

    The options live on the task rather than being passed to enqueue()
    because they describe the task's code - how flaky its dependency
    is, how long it can legitimately run - and the worker, which runs
    that code, is the one process guaranteed to have imported it. The
    app enqueueing it may only know the task's name.

    max_execution_seconds is how long the heartbeat keeps the job's
    lease alive, not a timeout: Python can't kill a thread, so a task
    past it keeps running, but its lease lapses and the job is recovered
    and run elsewhere.
    """
    def register(fn: Callable[..., Any]) -> Callable[..., Any]:
        if fn.__name__ in _registry:
            raise ValueError(f"task {fn.__name__!r} already registered")

        # A task that declares a `conn` parameter is run inside the worker's
        # transaction: its own writes and the job's completion commit
        # together, so a crash can never leave one without the other.
        # Inspected once here rather than on every invocation.
        wants_connection = "conn" in inspect.signature(fn).parameters

        _registry[fn.__name__] = RegisteredTask(
            fn=fn,
            wants_connection=wants_connection,
            max_attempts=max_attempts,
            max_execution_seconds=max_execution_seconds,
        )
        return fn

    if fn is None:
        return register
    return register(fn)


def get_registered_task(name: str) -> RegisteredTask:
    try:
        return _registry[name]
    except KeyError:
        raise KeyError(f"no task registered as {name!r}") from None


def get_task(name: str) -> Callable[..., Any]:
    return get_registered_task(name).fn
