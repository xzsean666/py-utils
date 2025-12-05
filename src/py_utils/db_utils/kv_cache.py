from __future__ import annotations

import asyncio
import json
import inspect
import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional, Protocol, TypeVar

from .memory_cache import _json_fallback  # reuse key serialization helper

logger = logging.getLogger(__name__)

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


class KVDatabase(Protocol[T]):
    """Minimal protocol for key-value stores used by cache decorator."""

    def get(self, key: str, ttl: Optional[int] = None) -> Optional[T]: ...

    def put(self, key: str, value: T) -> None: ...


def _make_cache_key(
    func_name: str, prefix: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> str:
    try:
        # 对于实例方法，args[0] 是 self，我们需要排除它
        # 因为 self 对象在序列化时可能无法正确区分不同的实例
        # 我们只使用其他参数和 kwargs 来生成缓存键
        # 这样确保 user_id 等参数能被正确包含在缓存键中
        cache_args = args
        if args:
            first_arg = args[0]
            # 检查是否是实例方法：第一个参数不是基本类型，且有 __class__ 属性
            is_instance_method = hasattr(first_arg, "__class__") and not isinstance(
                first_arg, (str, int, float, bool, type(None), list, dict, tuple)
            )

            if is_instance_method:
                # 完全排除 self，只使用其他参数
                # 这样 kwargs 中的 user_id 等参数会被正确包含
                cache_args = args[1:] if len(args) > 1 else ()

        payload = json.dumps(
            [cache_args, kwargs],
            default=_json_fallback,
            separators=(",", ":"),
            sort_keys=True,
        )
    except Exception:
        # 如果序列化失败，使用 repr，但也要处理实例方法
        if args:
            first_arg = args[0]
            is_instance_method = hasattr(first_arg, "__class__") and not isinstance(
                first_arg, (str, int, float, bool, type(None), list, dict, tuple)
            )
            if is_instance_method:
                cache_args = args[1:] if len(args) > 1 else ()
                payload = repr((cache_args, kwargs))
            else:
                payload = repr((args, kwargs))
        else:
            payload = repr((args, kwargs))

    base = f"{prefix}:{func_name}:{payload}" if prefix else f"{func_name}:{payload}"
    return base[:255]


class AsyncLockManager:
    """管理每个缓存 key 的异步锁，防止并发重复计算"""

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def get_lock(self, key: str) -> asyncio.Lock:
        """获取或创建指定 key 的锁"""
        if key not in self._locks:
            async with self._global_lock:
                if key not in self._locks:
                    self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def cleanup(self, max_locks: int = 10000):
        """清理过多的锁（简单策略：超过阈值时清空）"""
        if len(self._locks) > max_locks:
            self._locks.clear()


# 全局锁管理器
_lock_manager = AsyncLockManager()


def create_kv_cache(
    db: KVDatabase[T],
    default_ttl: int = 60,
    use_lock: bool = True,
) -> Callable[[int | None, str], Callable[[F], F]]:
    """
    Build a decorator that caches function results in a KV database.

    Args:
        db: KV database instance (e.g., SqliteKVDatabase)
        default_ttl: Default cache TTL in seconds
        use_lock: If True, use async locks to prevent concurrent duplicate computation
                  for the same cache key. Only affects async functions.

    Usage:
        cache = create_kv_cache(db_instance)

        @cache(ttl=120, prefix="user")
        def compute(x): ...

        # With locking for heavy computations
        @cache(ttl=120, prefix="heavy")
        async def heavy_compute(x): ...
    """

    def decorator(ttl: int | None = None, prefix: str = "") -> Callable[[F], F]:
        effective_ttl = default_ttl if ttl is None else ttl

        def wrapper(func: F) -> F:
            cache_key_name = func.__qualname__

            if inspect.iscoroutinefunction(func):
                if use_lock:
                    # Async function with lock - prevents concurrent duplicate computation
                    @wraps(func)
                    async def async_wrapper_with_lock(*args: Any, **kwargs: Any) -> Any:
                        key = _make_cache_key(cache_key_name, prefix, args, kwargs)

                        # Fast path: check cache without lock
                        try:
                            cached = db.get(key, effective_ttl)
                            if cached is not None:
                                logger.debug(f"Cache hit (fast path): {prefix}:{key[:50]}")
                                return cached
                        except Exception:
                            pass

                        # Get lock for this key
                        lock = await _lock_manager.get_lock(key)

                        async with lock:
                            # Double-check: another request may have computed while we waited
                            try:
                                cached = db.get(key, effective_ttl)
                                if cached is not None:
                                    logger.debug(f"Cache hit (after lock): {prefix}:{key[:50]}")
                                    return cached
                            except Exception:
                                pass

                            # Compute result
                            logger.debug(f"Cache miss, computing: {prefix}:{key[:50]}")
                            result = await func(*args, **kwargs)  # type: ignore[misc]

                            try:
                                db.put(key, result)
                            except Exception:
                                pass
                            return result

                    return async_wrapper_with_lock  # type: ignore[return-value]
                else:
                    # Async function without lock (original behavior)
                    @wraps(func)
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

                    return async_wrapper  # type: ignore[return-value]

            # Sync function (no lock needed - blocking by nature)
            @wraps(func)
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

            return sync_wrapper  # type: ignore[return-value]

        return wrapper

    return decorator
