"""Lightweight example script exercising SqliteKVDatabase.

Run with: uv run tests/kv_sqlite_example.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from py_utils.db_utils.kv_sqlite import SqliteKVDatabase


def run_example() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = "./db/kv_example.db"
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        db = SqliteKVDatabase(str(db_path), value_type="json")

        # Basic put/get
        db.put("user:1", {"name": "Ada", "age": 36})
        fetched = db.get("user:1", {"include_timestamps": True})
        assert fetched and fetched["value"]["name"] == "Ada"

        # Merge JSON
        db.merge("user:1", {"age": 37})
        assert db.get("user:1")["age"] == 37  # type: ignore[index]

        # Batch insert
        db.putMany([("user:2", {"name": "Bob"}), ("meta:info", {"count": 2})])
        assert db.count() == 3

        # Prefix scan
        prefix_results = db.getWithPrefix("user:", {"include_timestamps": False})
        assert len(prefix_results) == 2

        # Get many and delete
        many = db.getMany(["user:1", "user:2", "missing"])
        assert many["missing"] is None
        db.delete("user:2")
        assert not db.has("user:2")

        # Find by value and clear
        keys = db.findByValue({"count": 2}, exact=True)
        assert keys == ["meta:info"]
        db.clear()
        assert db.count() == 0

    print("SqliteKVDatabase example finished successfully.")


if __name__ == "__main__":
    run_example()
