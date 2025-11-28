from __future__ import annotations

import inspect
import json
import threading
import time
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Tuple, TypeVar

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


@dataclass
class _CacheEntry:
    value: Any
    expiry: float | None = None


class MemoryCache:
    """Simple in-memory cache with optional TTL support."""

    def __init__(self) -> None:
        self._store: dict[str, _CacheEntry] = {}
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def try_get(self, key: str, ttl: float | None = None) -> Tuple[bool, Any]:
        """
        Return (hit, value). If ttl is provided and the entry has no expiry,
        it will be upgraded to use the given ttl.
        """
        now = time.time()
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return False, None

            if entry.expiry is not None and entry.expiry <= now:
                self._delete_unlocked(key)
                return False, None

            if ttl and entry.expiry is None:
                entry.expiry = now + ttl
                self._schedule_cleanup_unlocked(key, ttl)

            return True, entry.value

    def get(self, key: str, ttl: float | None = None) -> Any | None:
        hit, value = self.try_get(key, ttl)
        return value if hit else None

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._cancel_timer_unlocked(key)
            self._store[key] = _CacheEntry(value=value)

    def put_with_ttl(self, key: str, value: Any, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            self.put(key, value)
            return

        expiry = time.time() + ttl_seconds
        with self._lock:
            self._cancel_timer_unlocked(key)
            self._store[key] = _CacheEntry(value=value, expiry=expiry)
            self._schedule_cleanup_unlocked(key, ttl_seconds)

    def clear(self) -> None:
        with self._lock:
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()
            self._store.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._store)

    def has(self, key: str) -> bool:
        hit, _ = self.try_get(key)
        return hit

    def _delete(self, key: str) -> None:
        with self._lock:
            self._delete_unlocked(key)

    def _delete_unlocked(self, key: str) -> None:
        self._store.pop(key, None)
        self._cancel_timer_unlocked(key)

    def _schedule_cleanup_unlocked(self, key: str, delay_seconds: float) -> None:
        timer = threading.Timer(delay_seconds, self._delete, args=(key,))
        timer.daemon = True
        self._timers[key] = timer
        timer.start()

    def _cancel_timer_unlocked(self, key: str) -> None:
        timer = self._timers.pop(key, None)
        if timer:
            timer.cancel()


def _json_fallback(value: Any) -> str:
    if hasattr(value, "__dict__"):
        try:
            return value.__dict__
        except Exception:
            pass
    return repr(value)


def _make_cache_key(
    func_name: str, prefix: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> str:
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


def _build_cache_decorator(
    store: MemoryCache, ttl: float, prefix: str
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        if ttl <= 0:
            return func

        cache_key_name = func.__qualname__

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                key = _make_cache_key(cache_key_name, prefix, args, kwargs)
                try:
                    hit, cached = store.try_get(key, ttl)
                except Exception:
                    return await func(*args, **kwargs)

                if hit:
                    return cached

                result = await func(*args, **kwargs)
                try:
                    store.put_with_ttl(key, result, ttl)
                except Exception:
                    pass
                return result

            return async_wrapper  # type: ignore[return-value]

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            key = _make_cache_key(cache_key_name, prefix, args, kwargs)
            try:
                hit, cached = store.try_get(key, ttl)
            except Exception:
                return func(*args, **kwargs)

            if hit:
                return cached

            result = func(*args, **kwargs)
            try:
                store.put_with_ttl(key, result, ttl)
            except Exception:
                pass
            return result

        return sync_wrapper  # type: ignore[return-value]

    return decorator


def create_memory_cache(default_ttl: float = 60.0) -> Callable[..., Callable[[F], F]]:
    store = MemoryCache()

    def decorator(ttl: float | None = None, prefix: str = "") -> Callable[[F], F]:
        effective_ttl = default_ttl if ttl is None else ttl
        return _build_cache_decorator(store, effective_ttl, prefix)

    return decorator


_global_cache = MemoryCache()


def memory_cache(ttl: float = 60.0, prefix: str = "") -> Callable[[F], F]:
    return _build_cache_decorator(_global_cache, ttl, prefix)


def clear_memory_cache() -> None:
    _global_cache.clear()


__all__ = [
    "MemoryCache",
    "memory_cache",
    "create_memory_cache",
    "clear_memory_cache",
]
