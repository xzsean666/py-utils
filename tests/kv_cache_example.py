"""Example script demonstrating the KV cache decorator.

Run with: uv run tests/kv_cache_example.py
"""

from __future__ import annotations

import asyncio

from py_utils.db_utils import SqliteKVDatabase, create_kv_cache
from py_utils.db_utils import sqlite_cache


def main() -> None:
    # Use in-memory sqlite for the demo
    # db = SqliteKVDatabase(value_type="json")
    # cache = create_kv_cache(db, default_ttl=5)
    cache = sqlite_cache

    @cache(prefix="math")
    def add(a: int, b: int) -> int:
        print("computed add")  # shows when cache miss occurs
        return a + b

    assert add(1, 2) == 3  # miss
    assert add(1, 2) == 3  # hit, "computed add" not printed

    @cache(prefix="async", ttl=2)
    async def slow_square(x: int) -> int:
        print("computed slow_square")  # shows on cache miss
        await asyncio.sleep(0.01)
        return x * x

    async def run_async() -> None:
        assert await slow_square(3) == 9  # miss
        assert await slow_square(3) == 9  # hit

    asyncio.run(run_async())
    print("KV cache example finished successfully.")


if __name__ == "__main__":
    main()
