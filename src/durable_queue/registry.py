from collections.abc import Callable
from typing import Any


_registry: dict[str, Callable[..., Any]] = {}

def task(fn: Callable[..., Any]) -> Callable[..., Any]:
    if fn.__name__ in _registry:
        raise ValueError(f"task {fn.__name__!r} already registered")
    _registry[fn.__name__] = fn
    return fn

def get_task(name: str) -> Callable[..., Any]:
    try:
        return _registry[name]
    except KeyError:
        raise KeyError(f"no task registered as {name!r}") from None