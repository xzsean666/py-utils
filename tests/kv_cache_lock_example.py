"""Example script demonstrating the async lock feature in KV cache.

This example shows that when multiple concurrent requests hit the same cache key,
only ONE request will actually compute the result, while others wait for the cache.

Run with: uv run tests/kv_cache_lock_example.py
"""

from __future__ import annotations

import asyncio
import time
from typing import List

from py_utils.db_utils import SqliteKVDatabase, create_kv_cache


# Track computation calls
computation_count = 0


async def test_concurrent_cache_with_lock() -> None:
    """Test that concurrent requests only trigger ONE computation with lock enabled."""
    global computation_count
    computation_count = 0

    # Create cache with lock enabled (default)
    db = SqliteKVDatabase(value_type="json")
    cache = create_kv_cache(db, default_ttl=60)

    @cache(prefix="heavy")
    async def heavy_computation(x: int) -> dict:
        """Simulate a heavy computation that takes 0.5 seconds."""
        global computation_count
        computation_count += 1
        print(f"  [Computing] heavy_computation({x}) - computation #{computation_count}")
        await asyncio.sleep(0.5)  # Simulate heavy work
        return {"result": x * x, "computed_at": time.time()}

    print("=" * 60)
    print("Test 1: Concurrent requests WITH lock (use_lock=True)")
    print("=" * 60)
    print("Launching 5 concurrent requests for the same key...")
    print()

    start = time.time()
    # Launch 5 concurrent requests for the same computation
    tasks = [heavy_computation(42) for _ in range(5)]
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    print()
    print(f"Results received: {len(results)} responses")
    print(f"Computation count: {computation_count} (should be 1)")
    print(f"Total time: {elapsed:.2f}s (should be ~0.5s, not 2.5s)")
    print(f"All results identical: {len(set(r['result'] for r in results)) == 1}")

    assert computation_count == 1, f"Expected 1 computation, got {computation_count}"
    assert all(r["result"] == 1764 for r in results), "All results should be 42*42=1764"
    print("✅ Test 1 PASSED: Only 1 computation with lock!\n")

    db.close()


async def test_concurrent_cache_without_lock() -> None:
    """Test that concurrent requests trigger MULTIPLE computations without lock."""
    global computation_count
    computation_count = 0

    # Create cache with lock DISABLED
    db = SqliteKVDatabase(value_type="json")
    cache = create_kv_cache(db, default_ttl=60, use_lock=False)

    @cache(prefix="heavy")
    async def heavy_computation(x: int) -> dict:
        """Simulate a heavy computation that takes 0.3 seconds."""
        global computation_count
        computation_count += 1
        print(f"  [Computing] heavy_computation({x}) - computation #{computation_count}")
        await asyncio.sleep(0.3)  # Simulate heavy work
        return {"result": x * x, "computed_at": time.time()}

    print("=" * 60)
    print("Test 2: Concurrent requests WITHOUT lock (use_lock=False)")
    print("=" * 60)
    print("Launching 5 concurrent requests for the same key...")
    print()

    start = time.time()
    # Launch 5 concurrent requests for the same computation
    tasks = [heavy_computation(42) for _ in range(5)]
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    print()
    print(f"Results received: {len(results)} responses")
    print(f"Computation count: {computation_count} (expected ~5 without lock)")
    print(f"Total time: {elapsed:.2f}s")
    print(f"All results identical: {len(set(r['result'] for r in results)) == 1}")

    # Without lock, all 5 requests compute simultaneously
    assert (
        computation_count > 1
    ), f"Expected multiple computations without lock, got {computation_count}"
    print("✅ Test 2 PASSED: Multiple computations without lock (as expected)!\n")

    db.close()


async def test_different_keys_parallel() -> None:
    """Test that different keys can be computed in parallel even with lock."""
    global computation_count
    computation_count = 0

    db = SqliteKVDatabase(value_type="json")
    cache = create_kv_cache(db, default_ttl=60, use_lock=True)

    @cache(prefix="parallel")
    async def compute(x: int) -> int:
        global computation_count
        computation_count += 1
        print(f"  [Computing] compute({x})")
        await asyncio.sleep(0.2)
        return x * x

    print("=" * 60)
    print("Test 3: Different keys compute in parallel (with lock)")
    print("=" * 60)
    print("Launching requests for keys 1, 2, 3, 4, 5 in parallel...")
    print()

    start = time.time()
    # Different keys should compute in parallel
    tasks = [compute(i) for i in range(1, 6)]
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    print()
    print(f"Results: {results}")
    print(f"Computation count: {computation_count} (should be 5)")
    print(f"Total time: {elapsed:.2f}s (should be ~0.2s since all run in parallel)")

    assert computation_count == 5, f"Expected 5 computations for 5 different keys"
    assert results == [1, 4, 9, 16, 25], f"Expected [1, 4, 9, 16, 25], got {results}"
    print("✅ Test 3 PASSED: Different keys compute in parallel!\n")

    db.close()


async def test_cache_hit_no_lock_needed() -> None:
    """Test that cache hits bypass the lock (fast path)."""
    global computation_count
    computation_count = 0

    db = SqliteKVDatabase(value_type="json")
    cache = create_kv_cache(db, default_ttl=60, use_lock=True)

    @cache(prefix="fastpath")
    async def compute(x: int) -> int:
        global computation_count
        computation_count += 1
        print(f"  [Computing] compute({x})")
        await asyncio.sleep(0.1)
        return x * x

    print("=" * 60)
    print("Test 4: Cache hit uses fast path (no lock)")
    print("=" * 60)

    # First call - computes and caches
    print("First call (cache miss)...")
    result1 = await compute(10)
    assert computation_count == 1

    # Subsequent calls - should hit cache without lock
    print("10 subsequent calls (should all hit cache)...")
    start = time.time()
    tasks = [compute(10) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    print()
    print(f"Computation count: {computation_count} (should still be 1)")
    print(f"Time for 10 cache hits: {elapsed:.4f}s (should be very fast)")
    print(f"All results correct: {all(r == 100 for r in results)}")

    assert computation_count == 1, "Cache hits should not trigger computation"
    assert elapsed < 0.1, "Cache hits should be very fast"
    print("✅ Test 4 PASSED: Cache hits use fast path!\n")

    db.close()


async def main() -> None:
    print("\n" + "=" * 60)
    print("KV Cache Async Lock Feature Demo")
    print("=" * 60 + "\n")

    await test_concurrent_cache_with_lock()
    await test_concurrent_cache_without_lock()
    await test_different_keys_parallel()
    await test_cache_hit_no_lock_needed()

    print("=" * 60)
    print("All tests passed! 🎉")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
