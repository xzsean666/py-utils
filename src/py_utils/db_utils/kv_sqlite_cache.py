from __future__ import annotations

from pathlib import Path

from .kv_cache import create_kv_cache
from .kv_sqlite import SqliteKVDatabase


def _init_db(path: Path) -> SqliteKVDatabase:
    path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteKVDatabase(str(path))


_default_db = _init_db(Path("./db/cache.db"))

# Default cache decorator using sqlite-backed store
cache = create_kv_cache(_default_db)

__all__ = ["cache", "_default_db", "create_kv_cache", "SqliteKVDatabase"]
