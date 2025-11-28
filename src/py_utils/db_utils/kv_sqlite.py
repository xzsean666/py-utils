from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple


SqliteValueType = Literal["json", "text", "blob", "integer", "real", "boolean"]


@dataclass(frozen=True)
class _TypeHandler:
    serialize: Callable[[Any], Any]
    deserialize: Callable[[Any], Any]
    column_type: str


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _serialize_blob(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError("BLOB type requires bytes-like or string input")


def _serialize_int(value: Any) -> int:
    number = int(value)
    if number != value and not isinstance(value, bool):
        # Preserve the stricter integer validation the TS version enforces
        raise ValueError("INTEGER type requires integer value")
    return number


TYPE_HANDLERS: Dict[SqliteValueType, _TypeHandler] = {
    "json": _TypeHandler(
        serialize=lambda value: json.dumps(value, default=_json_default, ensure_ascii=False),
        deserialize=lambda value: json.loads(value) if value is not None else None,
        column_type="TEXT",
    ),
    "text": _TypeHandler(
        serialize=lambda value: str(value),
        deserialize=lambda value: value,
        column_type="TEXT",
    ),
    "blob": _TypeHandler(
        serialize=_serialize_blob,
        deserialize=lambda value: bytes(value) if value is not None else None,
        column_type="BLOB",
    ),
    "integer": _TypeHandler(
        serialize=_serialize_int,
        deserialize=lambda value: int(value) if value is not None else 0,
        column_type="INTEGER",
    ),
    "real": _TypeHandler(
        serialize=lambda value: float(value),
        deserialize=lambda value: float(value) if value is not None else 0.0,
        column_type="REAL",
    ),
    "boolean": _TypeHandler(
        serialize=lambda value: 1 if value else 0,
        deserialize=lambda value: bool(value),
        column_type="INTEGER",
    ),
}


def _normalize_order(order: str | None) -> str:
    return "DESC" if str(order or "").upper() == "DESC" else "ASC"


def _to_timestamp(value: Any) -> float:
    if value is None:
        raise TypeError("timestamp value cannot be None")
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        # Accept milliseconds
        return float(value / 1000) if value > 1e12 else float(value)
    raise TypeError(f"Unsupported timestamp type: {type(value)}")


def _as_datetime(value: Any) -> datetime:
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


class SqliteKVDatabase:
    """SQLite key-value helper with multiple value type support."""

    def __init__(
        self,
        datasource_path: Optional[str] = None,
        table_name: str = "kv_store",
        value_type: SqliteValueType = "json",
    ) -> None:
        if '"' in table_name:
            raise ValueError('table name cannot contain quote characters')

        self.table_name = table_name
        self.value_type = value_type
        self._type_handler = TYPE_HANDLERS[value_type]
        self._quoted_table = f'"{self.table_name}"'
        self._conn = sqlite3.connect(
            datasource_path or ":memory:",
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._initialized = False
        self._init_lock = threading.Lock()
        self._lock = threading.Lock()

    # Internal helpers
    def _with_retry(self, operation: Callable[[], Any], retries: int = 2, delay: float = 0.1) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(retries):
            try:
                with self._lock:
                    return operation()
            except sqlite3.OperationalError as exc:  # pragma: no cover - small helper
                last_error = exc
                message = str(exc).lower()
                if ("locked" in message or "busy" in message) and attempt < retries - 1:
                    time.sleep(delay)
                    continue
                raise
            except sqlite3.DatabaseError as exc:
                last_error = exc
                raise
        if last_error:
            raise last_error

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return

            def setup() -> None:
                with self._conn:
                    self._conn.execute("PRAGMA journal_mode=WAL;")
                    self._conn.execute("PRAGMA busy_timeout=3000;")
                    self._conn.execute("PRAGMA synchronous=NORMAL;")
                    self._conn.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self._quoted_table} (
                            key TEXT PRIMARY KEY,
                            value {self._type_handler.column_type},
                            created_at REAL DEFAULT (strftime('%s','now')),
                            updated_at REAL DEFAULT (strftime('%s','now'))
                        );
                        """
                    )

            self._with_retry(setup)
            self._initialized = True

    def _serialize_value(self, value: Any) -> Any:
        return self._type_handler.serialize(value)

    def _deserialize_value(self, value: Any) -> Any:
        return self._type_handler.deserialize(value)

    def _row_to_value(
        self,
        row: sqlite3.Row,
        include_timestamps: bool,
    ) -> Any:
        value = self._deserialize_value(row["value"])
        if not include_timestamps:
            return value
        return {
            "value": value,
            "created_at": _as_datetime(row["created_at"]),
            "updated_at": _as_datetime(row["updated_at"]),
        }

    # Basic CRUD
    def put(self, key: str, value: Any) -> None:
        self._ensure_initialized()
        serialized = self._serialize_value(value)
        sql = f"""
        INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
        VALUES (?, ?, strftime('%s','now'), strftime('%s','now'))
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = strftime('%s','now');
        """

        def op() -> None:
            with self._conn:
                self._conn.execute(sql, (key, serialized))

        self._with_retry(op)

    def get(
        self,
        key: str,
        options_or_expire: Optional[int | Dict[str, Any]] = None,
    ) -> Optional[Any]:
        self._ensure_initialized()
        sql = f"""
        SELECT key, value, created_at, updated_at
        FROM {self._quoted_table}
        WHERE key = ?
        LIMIT 1;
        """

        row = self._with_retry(lambda: self._conn.execute(sql, (key,)).fetchone())
        if not row:
            return None

        expire: Optional[int] = None
        include_timestamps = False
        if isinstance(options_or_expire, int):
            expire = options_or_expire
        elif isinstance(options_or_expire, dict):
            expire = options_or_expire.get("expire")
            include_timestamps = bool(options_or_expire.get("include_timestamps"))

        if expire is not None:
            created_seconds = float(row["created_at"])
            if time.time() - created_seconds > expire:
                self.delete(key)
                return None

        return self._row_to_value(row, include_timestamps)

    def merge(self, key: str, value: Any) -> None:
        if self.value_type != "json":
            raise ValueError(f"Merge operation is only supported for JSON type, current type is: {self.value_type}")

        existing_value = self.get(key)
        if isinstance(existing_value, dict) and isinstance(value, dict):
            merged_value = {**existing_value, **value}
        else:
            merged_value = value
        self.put(key, merged_value)

    def delete(self, key: str) -> bool:
        self._ensure_initialized()
        sql = f"DELETE FROM {self._quoted_table} WHERE key = ?;"

        def op() -> int:
            with self._conn:
                cursor = self._conn.execute(sql, (key,))
            return cursor.rowcount

        affected = self._with_retry(op)
        return bool(affected and affected > 0)

    def add(self, key: str, value: Any) -> None:
        if self.has(key):
            raise ValueError(f'Key "{key}" already exists')
        self.put(key, value)

    def close(self) -> None:
        if self._initialized:
            self._conn.close()
            self._initialized = False

    # Bulk helpers
    def putMany(self, entries: List[Tuple[str, Any]], batch_size: int = 1000) -> None:
        if not entries:
            return
        self._ensure_initialized()

        for start in range(0, len(entries), batch_size):
            batch = entries[start : start + batch_size]
            values_sql = []
            params: List[Any] = []
            for key, value in batch:
                values_sql.append("(?, ?, strftime('%s','now'), strftime('%s','now'))")
                params.extend([key, self._serialize_value(value)])

            sql = f"""
            INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
            VALUES {",".join(values_sql)}
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = strftime('%s','now');
            """

            def op() -> None:
                with self._conn:
                    self._conn.execute(sql, params)

            self._with_retry(op)

    def deleteMany(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        self._ensure_initialized()
        placeholders = ",".join(["?"] * len(keys))
        sql = f"DELETE FROM {self._quoted_table} WHERE key IN ({placeholders});"

        def op() -> int:
            with self._conn:
                cursor = self._conn.execute(sql, tuple(keys))
            return cursor.rowcount or 0

        return self._with_retry(op)

    # Queries
    def getAll(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._ensure_initialized()
        opts = options or {}
        include_timestamps = bool(opts.get("include_timestamps"))

        where_clauses: List[str] = []
        params: List[Any] = []

        for field, op, opt_key in [
            ("created_at", ">=", "created_after"),
            ("created_at", "<=", "created_before"),
            ("updated_at", ">=", "updated_after"),
            ("updated_at", "<=", "updated_before"),
        ]:
            if opt_key in opts and opts[opt_key] is not None:
                where_clauses.append(f"{field} {op} ?")
                params.append(_to_timestamp(opts[opt_key]))

        sql = f"SELECT key, value, created_at, updated_at FROM {self._quoted_table}"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY key ASC"

        if opts.get("limit") is not None:
            sql += " LIMIT ?"
            params.append(int(opts["limit"]))
        if opts.get("offset") is not None:
            if opts.get("limit") is None:
                sql += " LIMIT -1"
            sql += " OFFSET ?"
            params.append(int(opts["offset"]))

        rows = self._with_retry(lambda: self._conn.execute(sql, params).fetchall())
        result: Dict[str, Any] = {}
        for row in rows:
            result[row["key"]] = self._row_to_value(row, include_timestamps)
        return result

    def getMany(
        self,
        keys: Sequence[str],
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not keys:
            return {}
        self._ensure_initialized()
        include_timestamps = bool((options or {}).get("include_timestamps"))
        placeholders = ",".join(["?"] * len(keys))
        sql = f"""
        SELECT key, value, created_at, updated_at
        FROM {self._quoted_table}
        WHERE key IN ({placeholders});
        """
        rows = self._with_retry(lambda: self._conn.execute(sql, tuple(keys)).fetchall())
        record_map = {
            row["key"]: self._row_to_value(row, include_timestamps) for row in rows
        }
        return {key: record_map.get(key, None) for key in keys}

    def getRecent(
        self,
        limit: int = 100,
        seconds: int = 0,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._ensure_initialized()
        include_timestamps = bool((options or {}).get("include_timestamps"))
        params: List[Any] = []
        sql = f"""
        SELECT key, value, created_at, updated_at
        FROM {self._quoted_table}
        """
        if seconds > 0:
            sql += "WHERE created_at >= ? "
            params.append(time.time() - seconds)
        sql += "ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = self._with_retry(lambda: self._conn.execute(sql, params).fetchall())
        result: Dict[str, Any] = {}
        for row in rows:
            result[row["key"]] = self._row_to_value(row, include_timestamps)
        return result

    def keys(self) -> List[str]:
        self._ensure_initialized()
        rows = self._with_retry(
            lambda: self._conn.execute(f"SELECT key FROM {self._quoted_table} ORDER BY key ASC").fetchall()
        )
        return [row["key"] for row in rows]

    def has(self, key: str) -> bool:
        self._ensure_initialized()
        sql = f"SELECT 1 FROM {self._quoted_table} WHERE key = ? LIMIT 1;"
        row = self._with_retry(lambda: self._conn.execute(sql, (key,)).fetchone())
        return bool(row)

    def clear(self) -> None:
        self._ensure_initialized()
        self._with_retry(lambda: self._conn.execute(f"DELETE FROM {self._quoted_table};"))
        self._conn.commit()

    def count(self) -> int:
        self._ensure_initialized()
        row = self._with_retry(
            lambda: self._conn.execute(f"SELECT COUNT(*) as count FROM {self._quoted_table};").fetchone()
        )
        return int(row["count"]) if row else 0

    def findByValue(self, value: Any, exact: bool = True) -> List[str]:
        self._ensure_initialized()
        if exact:
            serialized = self._serialize_value(value)
            sql = f"SELECT key FROM {self._quoted_table} WHERE value = ?;"
            rows = self._with_retry(lambda: self._conn.execute(sql, (serialized,)).fetchall())
            return [row["key"] for row in rows]

        if self.value_type not in ("text", "json"):
            raise ValueError(f"Fuzzy search not supported for {self.value_type} type")
        search_value = str(self._serialize_value(value))
        sql = f"SELECT key FROM {self._quoted_table} WHERE value LIKE ?;"
        rows = self._with_retry(lambda: self._conn.execute(sql, (f"%{search_value}%",)).fetchall())
        return [row["key"] for row in rows]

    def findByCondition(self, condition: Callable[[Any], bool]) -> Dict[str, Any]:
        self._ensure_initialized()
        rows = self._with_retry(
            lambda: self._conn.execute(f"SELECT key, value FROM {self._quoted_table};").fetchall()
        )
        result: Dict[str, Any] = {}
        for row in rows:
            value = self._deserialize_value(row["value"])
            if condition(value):
                result[row["key"]] = value
        return result

    def getValueType(self) -> SqliteValueType:
        return self.value_type

    def getTypeInfo(self) -> Dict[str, str]:
        return {"value_type": self.value_type, "column_type": self._type_handler.column_type}

    def getWithPrefix(
        self,
        prefix: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not prefix:
            raise ValueError("Prefix cannot be empty")
        self._ensure_initialized()
        opts = options or {}
        include_ts = bool(opts.get("include_timestamps"))
        limit = opts.get("limit")
        offset = opts.get("offset")
        order_by = _normalize_order(opts.get("order_by", "ASC"))

        sql = f"""
        SELECT key, value, created_at, updated_at
        FROM {self._quoted_table}
        WHERE key >= ? AND key < ?
        ORDER BY key {order_by}
        """
        params: List[Any] = [prefix, f"{prefix}\xFF"]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        if offset is not None:
            if limit is None:
                sql += " LIMIT -1"
            sql += " OFFSET ?"
            params.append(int(offset))

        rows = self._with_retry(lambda: self._conn.execute(sql, params).fetchall())
        results: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = _as_datetime(row["created_at"])
                item["updated_at"] = _as_datetime(row["updated_at"])
            results.append(item)
        return results
