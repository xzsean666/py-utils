import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Union

from psycopg2.extras import Json
from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.engine import Engine, Row
from sqlalchemy.pool import QueuePool

ValueType = Literal[
    "jsonb",
    "varchar",
    "text",
    "integer",
    "boolean",
    "float",
    "bytea",
]


def _to_datetime(timestamp: Union[int, float]) -> datetime:
    """Convert epoch milliseconds/seconds to timezone-aware datetime."""
    if timestamp > 1e12:
        return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


class PGKVDatabase:
    """PostgreSQL key-value helper with multiple value type support."""

    def __init__(
        self,
        datasource_url: str,
        table_name: str = "kv_store",
        value_type: ValueType = "jsonb",
    ) -> None:
        if not datasource_url:
            raise ValueError("datasource_url is required")
        if '"' in table_name:
            raise ValueError('table name cannot contain quote characters')
        self.table_name = table_name
        self.value_type = value_type
        self._initialized = False
        self._quoted_table = f'"{self.table_name}"'
        self._engine: Engine = create_engine(
            datasource_url,
            poolclass=QueuePool,
            pool_size=50,
            max_overflow=10,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 3},
        )
        self._logger = logging.getLogger(__name__)

    # Column and type helpers
    def _get_postgres_column_type(self) -> str:
        mapping = {
            "jsonb": "jsonb",
            "varchar": "varchar(255)",
            "text": "text",
            "integer": "integer",
            "boolean": "boolean",
            "float": "double precision",
            "bytea": "bytea",
        }
        return mapping.get(self.value_type, "jsonb")

    @staticmethod
    def _normalize_order(order_by: str) -> str:
        return "DESC" if str(order_by).upper() == "DESC" else "ASC"

    @staticmethod
    def _normalize_field(value: str, allowed: Sequence[str], default: str) -> str:
        value_str = str(value)
        return value_str if value_str in allowed else default

    @staticmethod
    def _validate_json_field(path: str) -> str:
        if not re.match(r"^[A-Za-z0-9_.:-]+$", path):
            raise ValueError(f"Invalid JSON path: {path}")
        return path

    # Serialization helpers
    def _serialize_value(self, value: Any) -> Any:
        if self.value_type == "jsonb":
            return Json(value)
        if self.value_type == "bytea":
            if isinstance(value, (bytes, bytearray, memoryview)):
                return bytes(value)
            if isinstance(value, str):
                return value.encode("utf-8")
            if isinstance(value, (list, dict)):
                return json.dumps(value).encode("utf-8")
            return str(value).encode("utf-8")
        return value

    def _deserialize_value(self, value: Any) -> Any:
        if self.value_type == "bytea" and isinstance(value, memoryview):
            return bytes(value)
        return value

    # Initialization and setup
    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._engine.begin() as conn:
            inspector = inspect(conn)
            if not inspector.has_table(self.table_name):
                conn.execute(
                    text(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self._quoted_table} (
                            key varchar(255) PRIMARY KEY,
                            value {self._get_postgres_column_type()},
                            created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
                            updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
                        );
                        """
                    )
                )
            if self.value_type == "jsonb":
                conn.execute(
                    text(
                        f'CREATE INDEX IF NOT EXISTS "IDX_{self.table_name}_value_gin" '
                        f"ON {self._quoted_table} USING gin (value);"
                    )
                )
                conn.execute(text("DROP FUNCTION IF EXISTS jsonb_deep_merge(jsonb, jsonb);"))
                conn.execute(
                    text(
                        """
                        CREATE OR REPLACE FUNCTION jsonb_deep_merge(a jsonb, b jsonb)
                        RETURNS jsonb AS $$
                        DECLARE
                          result jsonb;
                          key text;
                          value jsonb;
                        BEGIN
                          result := a;
                          FOR key, value IN SELECT * FROM jsonb_each(b)
                          LOOP
                            IF jsonb_typeof(result->key) = 'object' AND jsonb_typeof(value) = 'object' THEN
                              result := jsonb_set(result, array[key], jsonb_deep_merge(result->key, value));
                            ELSE
                              result := jsonb_set(result, array[key], value);
                            END IF;
                          END LOOP;
                          RETURN result;
                        END;
                        $$ LANGUAGE plpgsql;
                        """
                    )
                )
            else:
                conn.execute(
                    text(
                        f'CREATE INDEX IF NOT EXISTS "IDX_{self.table_name}_value_btree" '
                        f"ON {self._quoted_table} (value);"
                    )
                )
        self._initialized = True

    # Basic CRUD
    def put(self, key: str, value: Any) -> None:
        self._ensure_initialized()
        serialized = self._serialize_value(value)
        sql = f"""
        INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
        VALUES (:key, :value, NOW(), NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            updated_at = NOW();
        """
        with self._engine.begin() as conn:
            conn.execute(text(sql), {"key": key, "value": serialized})

    def merge(self, key: str, partial_value: Any) -> bool:
        if self.value_type != "jsonb":
            raise ValueError("merge is only supported for jsonb type")
        self._ensure_initialized()
        sql = f"""
        INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
        VALUES (:key, :value, NOW(), NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = CASE
            WHEN {self._quoted_table}.value IS NULL THEN EXCLUDED.value
            ELSE jsonb_deep_merge({self._quoted_table}.value, EXCLUDED.value)
        END,
        updated_at = NOW()
        RETURNING value;
        """
        with self._engine.begin() as conn:
            row = conn.execute(
                text(sql), {"key": key, "value": self._serialize_value(partial_value)}
            ).first()
        return bool(row)

    def get(
        self,
        key: str,
        options_or_expire: Optional[Union[int, Dict[str, Any]]] = None,
    ) -> Optional[Union[Any, Dict[str, Any]]]:
        self._ensure_initialized()
        sql = f"""
        SELECT key, value, created_at, updated_at
        FROM {self._quoted_table}
        WHERE key = :key
        LIMIT 1;
        """
        with self._engine.begin() as conn:
            row = conn.execute(text(sql), {"key": key}).mappings().first()
        if not row:
            return None

        expire = None
        include_timestamps = False
        if isinstance(options_or_expire, int):
            expire = options_or_expire
        elif isinstance(options_or_expire, dict):
            expire = options_or_expire.get("expire")
            include_timestamps = bool(options_or_expire.get("include_timestamps"))

        if expire is not None:
            created_time = row["created_at"]
            if isinstance(created_time, datetime):
                created_seconds = int(created_time.timestamp())
            else:
                created_seconds = int(created_time)
            if int(datetime.now(tz=timezone.utc).timestamp()) - created_seconds > expire:
                self.delete(key)
                return None

        value = self._deserialize_value(row["value"])
        if include_timestamps:
            return {
                "value": value,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        return value

    # Prefix/contains/suffix queries
    def getWithPrefix(
        self,
        prefix: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not prefix:
            raise ValueError("Prefix cannot be empty")
        self._ensure_initialized()
        opts = options or {}
        limit = opts.get("limit")
        offset = opts.get("offset")
        order_by = self._normalize_order(opts.get("order_by", "ASC"))
        include_ts = opts.get("include_timestamps", False)
        contains = opts.get("contains")
        case_sensitive = opts.get("case_sensitive", True)
        created_at_after = opts.get("created_at_after")
        created_at_before = opts.get("created_at_before")

        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"

        sql_parts = [
            f"SELECT {select_fields} FROM {self._quoted_table}",
            "WHERE key >= :start_prefix AND key < :end_prefix",
        ]
        params: Dict[str, Any] = {
            "start_prefix": prefix,
            "end_prefix": f"{prefix}\xFF",
        }

        has_time_filter = created_at_after is not None or created_at_before is not None
        if created_at_after is not None:
            sql_parts.append("AND created_at > :created_at_after")
            params["created_at_after"] = _to_datetime(created_at_after)
        if created_at_before is not None:
            sql_parts.append("AND created_at < :created_at_before")
            params["created_at_before"] = _to_datetime(created_at_before)

        sql_parts.append(f"ORDER BY key {order_by}")

        if not contains and not has_time_filter:
            if limit is not None:
                sql_parts.append("LIMIT :limit")
                params["limit"] = limit
            if offset is not None:
                sql_parts.append("OFFSET :offset")
                params["offset"] = offset

        sql = " ".join(sql_parts)
        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        results = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = row.get("created_at")
                item["updated_at"] = row.get("updated_at")
            results.append(item)

        if contains:
            search_term = contains if case_sensitive else contains.lower()
            filtered: List[Dict[str, Any]] = []
            for record in results:
                key_to_search = record["key"] if case_sensitive else record["key"].lower()
                if search_term in key_to_search:
                    filtered.append(record)
            results = filtered

        if contains or has_time_filter:
            if offset is not None:
                results = results[offset:]
            if limit is not None:
                results = results[:limit]

        return results

    def getWithContains(
        self,
        substring: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not substring:
            raise ValueError("Substring cannot be empty")
        self._ensure_initialized()
        opts = options or {}
        limit = opts.get("limit")
        offset = opts.get("offset")
        order_by = self._normalize_order(opts.get("order_by", "ASC"))
        case_sensitive = opts.get("case_sensitive", True)
        include_ts = opts.get("include_timestamps", False)

        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"

        like_operator = "LIKE" if case_sensitive else "ILIKE"
        sql_parts = [
            f"SELECT {select_fields} FROM {self._quoted_table}",
            f"WHERE key {like_operator} :pattern",
            f"ORDER BY key {order_by}",
        ]
        if limit is not None:
            sql_parts.append("LIMIT :limit")
        if offset is not None:
            sql_parts.append("OFFSET :offset")

        sql = " ".join(sql_parts)
        params = {"pattern": f"%{substring}%"}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        results: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = row.get("created_at")
                item["updated_at"] = row.get("updated_at")
            results.append(item)
        return results

    def getWithSuffix(
        self,
        suffix: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not suffix:
            raise ValueError("Suffix cannot be empty")
        self._ensure_initialized()
        opts = options or {}
        limit = opts.get("limit")
        offset = opts.get("offset")
        order_by = self._normalize_order(opts.get("order_by", "ASC"))
        case_sensitive = opts.get("case_sensitive", True)
        include_ts = opts.get("include_timestamps", False)

        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"

        like_operator = "LIKE" if case_sensitive else "ILIKE"
        sql_parts = [
            f"SELECT {select_fields} FROM {self._quoted_table}",
            f"WHERE key {like_operator} :pattern",
            f"ORDER BY key {order_by}",
        ]
        if limit is not None:
            sql_parts.append("LIMIT :limit")
        if offset is not None:
            sql_parts.append("OFFSET :offset")

        sql = " ".join(sql_parts)
        params = {"pattern": f"%{suffix}"}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        results: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = row.get("created_at")
                item["updated_at"] = row.get("updated_at")
            results.append(item)
        return results

    def getWithSuffixOptimized(
        self,
        suffix: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not suffix:
            raise ValueError("Suffix cannot be empty")
        reversed_suffix = "".join(reversed(suffix))
        reverse_prefix = f"reverse:{reversed_suffix}"
        reverse_results = self.getWithPrefix(reverse_prefix, options)
        if not reverse_results:
            return []
        original_keys: List[str] = []
        for record in reverse_results:
            value = record.get("value", {})
            if isinstance(value, dict) and "original_key" in value:
                original_keys.append(value["original_key"])
        if not original_keys:
            return []
        return self.getMany(original_keys)

    # Existence helpers
    def isValueExists(self, value: Any) -> bool:
        self._ensure_initialized()
        serialized = self._serialize_value(value)
        if self.value_type == "jsonb":
            sql = f"SELECT 1 FROM {self._quoted_table} WHERE value = :value LIMIT 1;"
        elif self.value_type == "bytea":
            sql = f"SELECT 1 FROM {self._quoted_table} WHERE value = :value LIMIT 1;"
        else:
            sql = f"SELECT 1 FROM {self._quoted_table} WHERE value = :value LIMIT 1;"
        with self._engine.begin() as conn:
            row = conn.execute(text(sql), {"value": serialized}).first()
        return bool(row)

    def getValues(self, value: Any) -> List[Dict[str, Any]]:
        self._ensure_initialized()
        serialized = self._serialize_value(value)
        sql = f"SELECT key, value, created_at, updated_at FROM {self._quoted_table} WHERE value = :value;"
        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), {"value": serialized}).mappings().all()
        return [
            {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
            }
            for row in rows
        ]

    def delete(self, key: str) -> bool:
        self._ensure_initialized()
        sql = f"DELETE FROM {self._quoted_table} WHERE key = :key;"
        with self._engine.begin() as conn:
            result = conn.execute(text(sql), {"key": key})
        return bool(result.rowcount and result.rowcount > 0)

    # Bulk operations
    def getMany(
        self,
        keys: Sequence[str],
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not keys:
            return []
        self._ensure_initialized()
        include_ts = bool((options or {}).get("include_timestamps"))
        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"
        stmt = text(
            f"SELECT {select_fields} FROM {self._quoted_table} WHERE key IN :keys"
        ).bindparams(bindparam("keys", expanding=True))
        with self._engine.begin() as conn:
            rows = conn.execute(stmt, {"keys": list(keys)}).mappings().all()
        results: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = row.get("created_at")
                item["updated_at"] = row.get("updated_at")
            results.append(item)
        return results

    def add(self, key: str, value: Any) -> None:
        self._ensure_initialized()
        if self.has(key):
            raise ValueError(f'Key "{key}" already exists')
        self.put(key, value)

    def addUniquePair(self, key: str, value: Any) -> None:
        self._ensure_initialized()
        serialized = self._serialize_value(value)
        sql = f"SELECT 1 FROM {self._quoted_table} WHERE key = :key AND value = :value LIMIT 1;"
        with self._engine.begin() as conn:
            existing = conn.execute(text(sql), {"key": key, "value": serialized}).first()
        if existing:
            raise ValueError(f'Key-value pair already exists for key "{key}"')
        self.put(key, value)

    def addUniqueValue(self, key: str, value: Any) -> None:
        self._ensure_initialized()
        if self.isValueExists(value):
            raise ValueError("Value already exists")
        self.put(key, value)

    def close(self) -> None:
        if self._initialized:
            self._engine.dispose()
            self._initialized = False

    def getAll(self, offset: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        self._ensure_initialized()
        sql_parts = [f"SELECT key, value FROM {self._quoted_table}"]
        params: Dict[str, Any] = {}
        if offset is not None:
            sql_parts.append("OFFSET :offset")
            params["offset"] = offset
        if limit is not None:
            sql_parts.append("LIMIT :limit")
            params["limit"] = limit
        sql = " ".join(sql_parts)
        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        return {row["key"]: self._deserialize_value(row["value"]) for row in rows}

    def keys(self) -> List[str]:
        self._ensure_initialized()
        sql = f"SELECT key FROM {self._quoted_table};"
        with self._engine.begin() as conn:
            rows = conn.execute(text(sql)).all()
        return [row[0] for row in rows]

    def has(self, key: str) -> bool:
        self._ensure_initialized()
        sql = f"SELECT 1 FROM {self._quoted_table} WHERE key = :key LIMIT 1;"
        with self._engine.begin() as conn:
            row = conn.execute(text(sql), {"key": key}).first()
        return bool(row)

    def putMany(self, entries: List[Tuple[str, Any]], batch_size: int = 1000) -> None:
        if not entries:
            return
        self._ensure_initialized()
        with self._engine.begin() as conn:
            for start in range(0, len(entries), batch_size):
                batch = entries[start : start + batch_size]
                values_sql = []
                params: Dict[str, Any] = {}
                for idx, (key, value) in enumerate(batch):
                    values_sql.append(
                        f"(:key{idx}, :value{idx}, NOW(), NOW())"
                    )
                    params[f"key{idx}"] = key
                    params[f"value{idx}"] = self._serialize_value(value)
                sql = f"""
                INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
                VALUES {",".join(values_sql)}
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
                """
                conn.execute(text(sql), params)

    def deleteMany(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        self._ensure_initialized()
        stmt = text(f"DELETE FROM {self._quoted_table} WHERE key IN :keys").bindparams(
            bindparam("keys", expanding=True)
        )
        with self._engine.begin() as conn:
            result = conn.execute(stmt, {"keys": list(keys)})
        return result.rowcount or 0

    def clear(self) -> None:
        self._ensure_initialized()
        with self._engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {self._quoted_table};"))

    def count(self) -> int:
        self._ensure_initialized()
        with self._engine.begin() as conn:
            row = conn.execute(text(f"SELECT COUNT(*) FROM {self._quoted_table};")).first()
        return int(row[0]) if row else 0

    # Specialized searches
    def findBoolValues(
        self,
        bool_value: bool,
        first: bool = True,
        order_by: str = "ASC",
    ) -> Union[str, List[Dict[str, Any]], None]:
        if self.value_type not in ("boolean", "jsonb"):
            raise ValueError("findBoolValues is only supported for boolean and jsonb types")
        self._ensure_initialized()
        order_by = self._normalize_order(order_by)
        select_fields = "key, value"
        sql_parts = [
            f"SELECT {select_fields} FROM {self._quoted_table}",
        ]
        if self.value_type == "jsonb":
            sql_parts.append("WHERE value = :value")
            params: Dict[str, Any] = {"value": self._serialize_value(bool_value)}
        else:
            sql_parts.append("WHERE value = :value")
            params = {"value": bool_value}
        sql_parts.append(f"ORDER BY created_at {order_by}")
        if first:
            sql_parts.append("LIMIT 1")
        sql = " ".join(sql_parts)
        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        if first:
            return rows[0]["key"] if rows else None
        return rows

    def searchJson(self, search_options: Dict[str, Any]) -> Dict[str, Any]:
        if self.value_type != "jsonb":
            raise ValueError("searchJson is only supported for jsonb type")
        self._ensure_initialized()
        limit = search_options.get("limit", 100)
        include_ts = bool(search_options.get("include_timestamps"))
        order_by = self._normalize_order(search_options.get("order_by", "ASC"))
        order_by_field = self._normalize_field(
            search_options.get("order_by_field", "key"),
            ["key", "created_at", "updated_at"],
            "key",
        )

        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"

        sql = f"SELECT {select_fields} FROM {self._quoted_table}"
        where_clauses: List[str] = []
        params: Dict[str, Any] = {}

        contains = search_options.get("contains") or {}
        for idx, (k, v) in enumerate(contains.items()):
            field = self._validate_json_field(k)
            where_clauses.append(f"value->>'{field}' = :contains_{idx}")
            params[f"contains_{idx}"] = str(v)

        compare = search_options.get("compare") or []
        for idx, cond in enumerate(compare):
            path = self._validate_json_field(cond["path"])
            op = cond["operator"]
            if op not in (">", "<", ">=", "<=", "=", "!="):
                raise ValueError(f"Unsupported operator: {op}")
            where_clauses.append(f"value->>'{path}' {op} :compare_{idx}")
            params[f"compare_{idx}"] = str(cond["value"])

        text_search = search_options.get("text_search") or []
        for idx, cond in enumerate(text_search):
            path = self._validate_json_field(cond["path"])
            like_operator = "LIKE" if cond.get("case_sensitive", False) else "ILIKE"
            where_clauses.append(f"value->>'{path}' {like_operator} :text_{idx}")
            params[f"text_{idx}"] = f"%{cond['text']}%"

        cursor = search_options.get("cursor")
        if cursor is not None:
            if order_by_field == "key":
                where_clauses.append("key > :cursor")
            else:
                where_clauses.append(f"{order_by_field} > :cursor")
            params["cursor"] = cursor

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        sql += f" ORDER BY {order_by_field} {order_by} LIMIT :limit_plus_one"
        params["limit_plus_one"] = limit + 1

        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        has_more = len(rows) > limit
        data = rows[:limit]
        next_cursor = data[-1][order_by_field] if has_more and data else None

        if not include_ts:
            for item in data:
                item.pop("created_at", None)
                item.pop("updated_at", None)

        return {"data": data, "next_cursor": next_cursor}

    def findByUpdateTime(
        self,
        timestamp: int,
        first: bool = True,
        type: str = "after",
        order_by: str = "ASC",
    ) -> Union[str, List[Dict[str, Any]], None]:
        self._ensure_initialized()
        order_by = self._normalize_order(order_by)
        operator = "<" if type == "before" else ">"
        sql = f"""
        SELECT key, value FROM {self._quoted_table}
        WHERE updated_at {operator} :timestamp
        ORDER BY updated_at {order_by}
        """
        if first:
            sql += " LIMIT 1"
        with self._engine.begin() as conn:
            rows = (
                conn.execute(text(sql), {"timestamp": _to_datetime(timestamp)})
                .mappings()
                .all()
            )
        if first:
            return rows[0]["key"] if rows else None
        return rows

    def searchByTime(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        self._ensure_initialized()
        time_column = params.get("time_column", "updated_at")
        include_ts = bool(params.get("include_timestamps"))
        time_column = self._normalize_field(time_column, ["updated_at", "created_at"], "updated_at")
        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"
        operator = "<" if params.get("type", "after") == "before" else ">"
        sql = f"""
        SELECT {select_fields} FROM {self._quoted_table}
        WHERE {time_column} {operator} :timestamp
        ORDER BY {time_column} {self._normalize_order(params.get("order_by", "ASC"))}
        LIMIT :take
        """
        with self._engine.begin() as conn:
            rows = (
                conn.execute(
                    text(sql),
                    {"timestamp": _to_datetime(params["timestamp"]), "take": params.get("take", 1)},
                )
                .mappings()
                .all()
            )
        results: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = row.get("created_at")
                item["updated_at"] = row.get("updated_at")
            results.append(item)
        return results

    def searchJsonByTime(
        self,
        search_options: Dict[str, Any],
        time_options: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if self.value_type != "jsonb":
            raise ValueError("searchJsonByTime is only supported for jsonb type")
        self._ensure_initialized()
        time_column = self._normalize_field(
            time_options.get("time_column", "updated_at"),
            ["updated_at", "created_at"],
            "updated_at",
        )
        include_ts = bool(time_options.get("include_timestamps"))
        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"

        sql_parts = [
            f"SELECT {select_fields} FROM {self._quoted_table}",
            f"WHERE {time_column} {'<' if time_options.get('type', 'after') == 'before' else '>'} :timestamp",
        ]
        params: Dict[str, Any] = {"timestamp": _to_datetime(time_options["timestamp"])}

        if search_options.get("contains") is not None:
            sql_parts.append("AND value @> :contains")
            params["contains"] = self._serialize_value(search_options["contains"])
        if search_options.get("equals") is not None:
            sql_parts.append("AND value = :equals")
            params["equals"] = self._serialize_value(search_options["equals"])
        if search_options.get("path") is not None and "value" in search_options:
            path = self._validate_json_field(search_options["path"])
            sql_parts.append(f"AND value #>> :path::text[] = :path_value")
            params["path"] = f"{{{path}}}"
            params["path_value"] = str(search_options["value"])

        sql_parts.append(
            f"ORDER BY {time_column} {self._normalize_order(time_options.get('order_by', 'ASC'))} LIMIT :take"
        )
        params["take"] = time_options.get("take", 1)

        sql = " ".join(sql_parts)
        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        results: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = row.get("created_at")
                item["updated_at"] = row.get("updated_at")
            results.append(item)
        return results

    # Array helpers (batch storage)
    def saveArray(
        self,
        key: str,
        array: List[Any],
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        opts = options or {}
        batch_size = opts.get("batch_size", 1000)
        force_update_batch_size = opts.get("force_update_batch_size", False)
        overwrite = opts.get("overwrite", False)

        self._ensure_initialized()
        meta_key = f"{key}_meta"
        existing_meta = self.get(meta_key)
        if not isinstance(existing_meta, dict):
            existing_meta = None

        if existing_meta and existing_meta.get("batch_count", 0) > 0 and not overwrite:
            stored_batch_size = existing_meta.get("batch_size", batch_size)
            active_batch_size = stored_batch_size
            if force_update_batch_size and batch_size != stored_batch_size:
                all_data = self.getAllArray(key)
                keys_to_delete = [meta_key] + [
                    f"{key}_{i}" for i in range(existing_meta["batch_count"])
                ]
                self.deleteMany(keys_to_delete)
                merged_array = all_data + array
                self.saveArray(
                    key,
                    merged_array,
                    {"batch_size": batch_size, "overwrite": True},
                )
                return
            batch_size = active_batch_size
            last_batch_key = f"{key}_{existing_meta['batch_count'] - 1}"
            last_batch = self.get(last_batch_key) or []
            remaining_space = batch_size - len(last_batch)
            items_for_last_batch = array[:remaining_space] if remaining_space > 0 else []
            remaining_items = array[remaining_space:] if remaining_space > 0 else array

            with self._engine.begin() as conn:
                if items_for_last_batch:
                    updated_last_batch = list(last_batch) + list(items_for_last_batch)
                    conn.execute(
                        text(
                            f"UPDATE {self._quoted_table} SET value = :value, updated_at = NOW() WHERE key = :key"
                        ),
                        {
                            "key": last_batch_key,
                            "value": self._serialize_value(updated_last_batch),
                        },
                    )

                new_batches = 0
                if remaining_items:
                    values_sql = []
                    params: Dict[str, Any] = {}
                    for idx in range(0, len(remaining_items), batch_size):
                        batch_items = remaining_items[idx : idx + batch_size]
                        batch_key = f"{key}_{existing_meta['batch_count'] + new_batches}"
                        values_sql.append(f"(:key{idx}, :value{idx}, NOW(), NOW())")
                        params[f"key{idx}"] = batch_key
                        params[f"value{idx}"] = self._serialize_value(batch_items)
                        new_batches += 1
                    conn.execute(
                        text(
                            f"INSERT INTO {self._quoted_table} (key, value, created_at, updated_at) "
                            f"VALUES {','.join(values_sql)}"
                        ),
                        params,
                    )

                updated_meta = {
                    "batch_count": existing_meta["batch_count"] + new_batches,
                    "total_items": existing_meta["total_items"] + len(array),
                    "batch_size": batch_size,
                    "last_updated": datetime.now(tz=timezone.utc).isoformat(),
                }
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
                        VALUES (:key, :value, NOW(), NOW())
                        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                        """
                    ),
                    {"key": meta_key, "value": self._serialize_value(updated_meta)},
                )
        else:
            with self._engine.begin() as conn:
                if overwrite:
                    keys_to_delete = [meta_key]
                    if existing_meta and existing_meta.get("batch_count", 0) > 0:
                        keys_to_delete.extend(
                            [f"{key}_{i}" for i in range(existing_meta["batch_count"])]
                        )
                    if keys_to_delete:
                        self.deleteMany(keys_to_delete)

                values_sql = []
                params: Dict[str, Any] = {}
                batch_count = 0
                for idx in range(0, len(array), batch_size):
                    batch_items = array[idx : idx + batch_size]
                    values_sql.append(f"(:key{idx}, :value{idx}, NOW(), NOW())")
                    params[f"key{idx}"] = f"{key}_{batch_count}"
                    params[f"value{idx}"] = self._serialize_value(batch_items)
                    batch_count += 1
                if values_sql:
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
                            VALUES {','.join(values_sql)}
                            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                            """
                        ),
                        params,
                    )

                meta_data = {
                    "batch_count": batch_count,
                    "total_items": len(array),
                    "batch_size": batch_size,
                    "last_updated": datetime.now(tz=timezone.utc).isoformat(),
                }
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {self._quoted_table} (key, value, created_at, updated_at)
                        VALUES (:key, :value, NOW(), NOW())
                        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                        """
                    ),
                    {"key": meta_key, "value": self._serialize_value(meta_data)},
                )

    def getAllArray(self, key: str) -> List[Any]:
        self._ensure_initialized()
        meta_key = f"{key}_meta"
        meta = self.get(meta_key)
        if not isinstance(meta, dict) or not meta.get("batch_count"):
            return []
        batch_keys = [f"{key}_{i}" for i in range(meta["batch_count"])]
        stmt = text(
            f"SELECT key, value FROM {self._quoted_table} WHERE key IN :keys ORDER BY key ASC"
        ).bindparams(bindparam("keys", expanding=True))
        with self._engine.begin() as conn:
            rows = conn.execute(stmt, {"keys": batch_keys}).mappings().all()
        batch_map = {row["key"]: row["value"] for row in rows}
        all_data: List[Any] = []
        for i in range(meta["batch_count"]):
            batch_key = f"{key}_{i}"
            batch = batch_map.get(batch_key) or []
            all_data.extend(batch)
        return all_data

    def getRecentArray(self, key: str, count: int, offset: int = 0) -> List[Any]:
        self._ensure_initialized()
        meta_key = f"{key}_meta"
        meta = self.get(meta_key)
        if not isinstance(meta, dict) or not meta.get("batch_count") or count <= 0:
            return []
        if offset >= meta["total_items"]:
            return []
        batch_size = meta.get("batch_size", 1000)
        total_needed = count + offset
        if total_needed >= meta["total_items"]:
            all_items = self.getAllArray(key)
            return all_items[max(0, len(all_items) - total_needed) : len(all_items) - offset]

        items_needed = total_needed
        start_batch = meta["batch_count"] - 1
        needed_batches: List[str] = []
        while items_needed > 0 and start_batch >= 0:
            needed_batches.append(f"{key}_{start_batch}")
            items_needed -= batch_size
            start_batch -= 1

        stmt = text(
            f"SELECT key, value FROM {self._quoted_table} WHERE key IN :keys ORDER BY key DESC"
        ).bindparams(bindparam("keys", expanding=True))
        with self._engine.begin() as conn:
            rows = conn.execute(stmt, {"keys": needed_batches}).mappings().all()

        all_recent_items: List[Any] = []
        remaining_count = total_needed
        for row in rows:
            batch = row["value"] or []
            if len(batch) <= remaining_count:
                all_recent_items[0:0] = batch
                remaining_count -= len(batch)
            else:
                start_idx = len(batch) - remaining_count
                all_recent_items[0:0] = batch[start_idx:]
                remaining_count = 0
            if remaining_count <= 0:
                break

        return all_recent_items[: len(all_recent_items) - offset]

    def getArrayRange(self, key: str, start_index: int, end_index: int) -> List[Any]:
        self._ensure_initialized()
        if start_index < 0 or end_index <= start_index:
            return []
        meta_key = f"{key}_meta"
        meta = self.get(meta_key)
        if not isinstance(meta, dict) or not meta.get("batch_count"):
            return []
        end_index = min(end_index, meta["total_items"])
        if start_index >= meta["total_items"]:
            return []
        batch_size = meta.get("batch_size", 1000)
        start_batch = start_index // batch_size
        end_batch = (end_index - 1) // batch_size
        batch_keys = [f"{key}_{i}" for i in range(start_batch, end_batch + 1)]
        stmt = text(
            f"SELECT key, value FROM {self._quoted_table} WHERE key IN :keys ORDER BY key ASC"
        ).bindparams(bindparam("keys", expanding=True))
        with self._engine.begin() as conn:
            rows = conn.execute(stmt, {"keys": batch_keys}).mappings().all()
        batch_map = {row["key"]: row["value"] for row in rows}
        result: List[Any] = []
        for i in range(start_batch, end_batch + 1):
            batch_key = f"{key}_{i}"
            batch = batch_map.get(batch_key) or []
            batch_start_index = i * batch_size
            local_start = max(0, start_index - batch_start_index)
            local_end = min(len(batch), end_index - batch_start_index)
            if local_start < local_end:
                result.extend(batch[local_start:local_end])
        return result

    def getRandomData(
        self,
        count: int = 1,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        self._ensure_initialized()
        include_ts = bool((options or {}).get("include_timestamps"))
        select_fields = "key, value"
        if include_ts:
            select_fields += ", created_at, updated_at"
        sql = f"""
        SELECT {select_fields} FROM {self._quoted_table}
        ORDER BY RANDOM()
        LIMIT :count;
        """
        with self._engine.begin() as conn:
            rows = conn.execute(text(sql), {"count": count}).mappings().all()
        results: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {
                "key": row["key"],
                "value": self._deserialize_value(row["value"]),
            }
            if include_ts:
                item["created_at"] = row.get("created_at")
                item["updated_at"] = row.get("updated_at")
            results.append(item)
        return results

    # Info helpers
    def getValueType(self) -> ValueType:
        return self.value_type

    def getTableName(self) -> str:
        return self.table_name

    def isOperationSupported(self, operation: str) -> bool:
        operation_type_map: Dict[str, Sequence[ValueType]] = {
            "merge": ["jsonb"],
            "searchJson": ["jsonb"],
            "searchJsonByTime": ["jsonb"],
            "findBoolValues": ["boolean", "jsonb"],
            "saveArray": ["jsonb"],
            "getAllArray": ["jsonb"],
            "getRecentArray": ["jsonb"],
            "getArrayRange": ["jsonb"],
        }
        supported_types = operation_type_map.get(operation)
        if not supported_types:
            return True
        return self.value_type in supported_types
