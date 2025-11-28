from __future__ import annotations

import json
import inspect
from functools import wraps
from typing import Any, Callable, Optional, Protocol, TypeVar

from .memory_cache import _json_fallback  # reuse key serialization helper


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


class KVDatabase(Protocol[T]):
    """Minimal protocol for key-value stores used by cache decorator."""

    def get(self, key: str, ttl: Optional[int] = None) -> Optional[T]:
        ...

    def put(self, key: str, value: T) -> None:
        ...


def _make_cache_key(func_name: str, prefix: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    try:
        payload = json.dumps(
            [args, kwargs],
            default=_json_fallback,
            separators=(",", ":"),
            sort_keys=True,
        )
    except Exception:
        payload = repr((args, kwargs))

    base = f"{prefix}:{func_name}:{payload}" if prefix else f"{func_name}:{payload}"
    return base[:255]


def create_kv_cache(db: KVDatabase[T], default_ttl: int = 60) -> Callable[[int | None, str], Callable[[F], F]]:
    """
    Build a decorator that caches function results in a KV database.

    Usage:
        cache = create_kv_cache(db_instance)

        @cache(ttl=120, prefix="user")
        def compute(x): ...
    """

    def decorator(ttl: int | None = None, prefix: str = "") -> Callable[[F], F]:
        effective_ttl = default_ttl if ttl is None else ttl

        def wrapper(func: F) -> F:
            cache_key_name = func.__qualname__

            if inspect.iscoroutinefunction(func):
                # Async function
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    key = _make_cache_key(cache_key_name, prefix, args, kwargs)
                    try:
                        cached = db.get(key, effective_ttl)
                        if cached is not None:
                            return cached
                    except Exception:
                        pass

                    result = await func(*args, **kwargs)  # type: ignore[misc]
                    try:
                        db.put(key, result)
                    except Exception:
                        pass
                    return result

                return wraps(func)(async_wrapper)  # type: ignore[return-value]

            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                key = _make_cache_key(cache_key_name, prefix, args, kwargs)
                try:
                    cached = db.get(key, effective_ttl)
                    if cached is not None:
                        return cached
                except Exception:
                    pass

                result = func(*args, **kwargs)
                try:
                    db.put(key, result)
                except Exception:
                    pass
                return result

            return wraps(func)(sync_wrapper)  # type: ignore[return-value]

        return wrapper

    return decorator
