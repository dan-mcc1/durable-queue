import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RegisteredTask:
    fn: Callable[..., Any]
    wants_connection: bool


_registry: dict[str, RegisteredTask] = {}


def task(fn: Callable[..., Any]) -> Callable[..., Any]:
    if fn.__name__ in _registry:
        raise ValueError(f"task {fn.__name__!r} already registered")

    # A task that declares a `conn` parameter is run inside the worker's
    # transaction: its own writes and the job's completion commit
    # together, so a crash can never leave one without the other.
    # Inspected once here rather than on every invocation.
    wants_connection = "conn" in inspect.signature(fn).parameters

    _registry[fn.__name__] = RegisteredTask(fn=fn, wants_connection=wants_connection)
    return fn


def get_registered_task(name: str) -> RegisteredTask:
    try:
        return _registry[name]
    except KeyError:
        raise KeyError(f"no task registered as {name!r}") from None


def get_task(name: str) -> Callable[..., Any]:
    return get_registered_task(name).fn
