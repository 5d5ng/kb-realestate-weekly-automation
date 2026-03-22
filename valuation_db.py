from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import threading

from db_backend import DEFAULT_DB_PATH, db_connection

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DEFAULT_DB_PATH
_SCHEMA_READY = False
_SCHEMA_LOCK = threading.Lock()


def _utcnow_text() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    if isinstance(row, dict):
        return dict(row)
    raise TypeError(f"지원하지 않는 row 타입입니다: {type(row)!r}")


def _loads_raw_json(value: Any) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        loaded = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    if isinstance(loaded, dict):
        return loaded
    return None


def _fetchall_dicts(cursor: Any) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description or []]
    result: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            result.append(_row_to_dict(row))
        else:
            result.append(dict(zip(columns, row)))
    return result


def _fetchone_dict(cursor: Any) -> dict[str, Any] | None:
    row = cursor.fetchone()
    if row is None:
        return None
    if hasattr(row, "keys"):
        return _row_to_dict(row)
    columns = [column[0] for column in cursor.description or []]
    return dict(zip(columns, row))


def ensure_db() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with db_connection(DB_PATH) as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except Exception:
                pass
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS complexes (
                    complex_id INTEGER PRIMARY KEY,
                    complex_name TEXT NOT NULL,
                    sido_name TEXT,
                    sigungu_name TEXT,
                    dong_name TEXT,
                    dong_code TEXT,
                    address TEXT,
                    households INTEGER,
                    completion_year INTEGER,
                    entrance_type TEXT,
                    raw_json TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS complex_types (
                    area_id INTEGER PRIMARY KEY,
                    complex_id INTEGER NOT NULL,
                    type_name TEXT,
                    exclusive_area REAL,
                    exclusive_area_pyeong REAL,
                    supply_area REAL,
                    supply_area_pyeong REAL,
                    contract_area REAL,
                    contract_area_pyeong REAL,
                    households INTEGER,
                    room_count INTEGER,
                    bathroom_count INTEGER,
                    total_trade_count INTEGER,
                    total_jeonse_count INTEGER,
                    total_monthly_rent_count INTEGER,
                    raw_json TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (complex_id) REFERENCES complexes(complex_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS price_history_cache (
                    complex_id INTEGER NOT NULL,
                    area_id INTEGER NOT NULL,
                    month_key TEXT NOT NULL,
                    month_label TEXT,
                    sale_general_price INTEGER,
                    jeonse_general_price INTEGER,
                    sale_upper_price INTEGER,
                    sale_lower_price INTEGER,
                    jeonse_upper_price INTEGER,
                    jeonse_lower_price INTEGER,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (complex_id, area_id, month_key),
                    FOREIGN KEY (complex_id) REFERENCES complexes(complex_id) ON DELETE CASCADE,
                    FOREIGN KEY (area_id) REFERENCES complex_types(area_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_complexes_name ON complexes(complex_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_complexes_region ON complexes(sido_name, sigungu_name, dong_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_complex_types_complex_id ON complex_types(complex_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_complex_area ON price_history_cache(complex_id, area_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_cache (
                    cache_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    expires_at TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_kv_cache_expires_at ON kv_cache(expires_at)")
        _SCHEMA_READY = True


def upsert_complexes(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    ensure_db()
    now = _utcnow_text()
    with db_connection(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT INTO complexes (
                complex_id,
                complex_name,
                sido_name,
                sigungu_name,
                dong_name,
                dong_code,
                address,
                households,
                completion_year,
                entrance_type,
                raw_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(complex_id) DO UPDATE SET
                complex_name=excluded.complex_name,
                sido_name=excluded.sido_name,
                sigungu_name=excluded.sigungu_name,
                dong_name=excluded.dong_name,
                dong_code=excluded.dong_code,
                address=excluded.address,
                households=excluded.households,
                completion_year=excluded.completion_year,
                entrance_type=excluded.entrance_type,
                raw_json=excluded.raw_json,
                updated_at=excluded.updated_at
            """,
            [
                (
                    row["complex_id"],
                    row["complex_name"],
                    row.get("sido_name"),
                    row.get("sigungu_name"),
                    row.get("dong_name"),
                    row.get("dong_code"),
                    row.get("address"),
                    row.get("households"),
                    row.get("completion_year"),
                    row.get("entrance_type"),
                    json.dumps(row.get("raw_json") or {}, ensure_ascii=False),
                    now,
                )
                for row in rows
            ],
        )
    return len(rows)


def upsert_complex_types(complex_id: int, rows: list[dict[str, Any]]) -> int:
    ensure_db()
    now = _utcnow_text()
    with db_connection(DB_PATH) as conn:
        conn.execute("DELETE FROM complex_types WHERE complex_id = ?", (complex_id,))
        if rows:
            conn.executemany(
                """
                INSERT INTO complex_types (
                    area_id,
                    complex_id,
                    type_name,
                    exclusive_area,
                    exclusive_area_pyeong,
                    supply_area,
                    supply_area_pyeong,
                    contract_area,
                    contract_area_pyeong,
                    households,
                    room_count,
                    bathroom_count,
                    total_trade_count,
                    total_jeonse_count,
                    total_monthly_rent_count,
                    raw_json,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["area_id"],
                        complex_id,
                        row.get("type_name"),
                        row.get("exclusive_area"),
                        row.get("exclusive_area_pyeong"),
                        row.get("supply_area"),
                        row.get("supply_area_pyeong"),
                        row.get("contract_area"),
                        row.get("contract_area_pyeong"),
                        row.get("households"),
                        row.get("room_count"),
                        row.get("bathroom_count"),
                        row.get("total_trade_count"),
                        row.get("total_jeonse_count"),
                        row.get("total_monthly_rent_count"),
                        json.dumps(row.get("raw_json") or {}, ensure_ascii=False),
                        now,
                    )
                    for row in rows
                ],
            )
    return len(rows)


def replace_price_history(
    complex_id: int,
    area_id: int,
    rows: list[dict[str, Any]],
    *,
    source: str,
) -> int:
    ensure_db()
    now = _utcnow_text()
    with db_connection(DB_PATH) as conn:
        conn.execute(
            "DELETE FROM price_history_cache WHERE complex_id = ? AND area_id = ?",
            (complex_id, area_id),
        )
        if rows:
            conn.executemany(
                """
                INSERT INTO price_history_cache (
                    complex_id,
                    area_id,
                    month_key,
                    month_label,
                    sale_general_price,
                    jeonse_general_price,
                    sale_upper_price,
                    sale_lower_price,
                    jeonse_upper_price,
                    jeonse_lower_price,
                    source,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        complex_id,
                        area_id,
                        row["month_key"],
                        row.get("month_label"),
                        row.get("sale_general_price"),
                        row.get("jeonse_general_price"),
                        row.get("sale_upper_price"),
                        row.get("sale_lower_price"),
                        row.get("jeonse_upper_price"),
                        row.get("jeonse_lower_price"),
                        source,
                        now,
                    )
                    for row in rows
                ],
            )
    return len(rows)


def search_complexes(query: str, limit: int = 20) -> list[dict[str, Any]]:
    ensure_db()
    query = (query or "").strip()
    if not query:
        return []

    like = f"%{query}%"
    with db_connection(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT
                complex_id,
                complex_name,
                sido_name,
                sigungu_name,
                dong_name,
                dong_code,
                address,
                households,
                completion_year,
                entrance_type,
                updated_at
            FROM complexes
            WHERE complex_name LIKE ?
               OR sigungu_name LIKE ?
               OR dong_name LIKE ?
               OR address LIKE ?
            ORDER BY complex_name ASC
            LIMIT ?
            """,
            (like, like, like, like, limit),
        )
        return _fetchall_dicts(cursor)


def get_complex(complex_id: int) -> dict[str, Any] | None:
    ensure_db()
    with db_connection(DB_PATH) as conn:
        cursor = conn.execute("SELECT * FROM complexes WHERE complex_id = ?", (complex_id,))
        row = _fetchone_dict(cursor)
    if not row:
        return None
    result = dict(row)
    result["raw_json"] = _loads_raw_json(result.get("raw_json"))
    return result


def get_complex_types(complex_id: int) -> list[dict[str, Any]]:
    ensure_db()
    with db_connection(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT *
            FROM complex_types
            WHERE complex_id = ?
            ORDER BY exclusive_area ASC, supply_area ASC, area_id ASC
            """,
            (complex_id,),
        )
        rows = _fetchall_dicts(cursor)
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["raw_json"] = _loads_raw_json(item.get("raw_json"))
        result.append(item)
    return result


def get_price_history(complex_id: int, area_id: int) -> list[dict[str, Any]]:
    ensure_db()
    with db_connection(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT *
            FROM price_history_cache
            WHERE complex_id = ? AND area_id = ?
            ORDER BY month_key DESC
            """,
            (complex_id, area_id),
        )
        return _fetchall_dicts(cursor)


def count_cached_complexes() -> int:
    ensure_db()
    with db_connection(DB_PATH) as conn:
        cursor = conn.execute("SELECT COUNT(*) AS count FROM complexes")
        row = _fetchone_dict(cursor)
    return int(row["count"] if row else 0)


def get_complexes_by_scope(
    sido_name: str,
    sigungu_name: str,
    *,
    max_age_hours: int | None = None,
) -> list[dict[str, Any]]:
    ensure_db()
    with db_connection(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT *
            FROM complexes
            WHERE sido_name = ? AND sigungu_name = ?
            ORDER BY complex_name ASC
            """,
            (sido_name, sigungu_name),
        )
        rows = _fetchall_dicts(cursor)

    if not rows:
        return []
    if max_age_hours is not None:
        cutoff = datetime.utcnow().timestamp() - (max_age_hours * 3600)
        latest_updated = max(
            datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S").timestamp()
            for row in rows
            if row.get("updated_at")
        )
        if latest_updated < cutoff:
            return []

    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["raw_json"] = _loads_raw_json(item.get("raw_json"))
        normalized.append(item)
    return normalized


def get_json_cache(cache_key: str) -> Any | None:
    ensure_db()
    with db_connection(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT payload_json, expires_at
            FROM kv_cache
            WHERE cache_key = ?
            """,
            (cache_key,),
        )
        row = _fetchone_dict(cursor)
    if not row:
        return None
    expires_at = row.get("expires_at")
    if expires_at:
        expires_dt = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
        if expires_dt < datetime.utcnow():
            return None
    try:
        return json.loads(row["payload_json"])
    except json.JSONDecodeError:
        return None


def set_json_cache(cache_key: str, payload: Any, *, ttl_seconds: int | None = None) -> None:
    ensure_db()
    now = datetime.utcnow()
    expires_at = None
    if ttl_seconds is not None:
        expires_at = (now + timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    with db_connection(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO kv_cache (cache_key, payload_json, expires_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                payload_json=excluded.payload_json,
                expires_at=excluded.expires_at,
                updated_at=excluded.updated_at
            """,
            (
                cache_key,
                json.dumps(payload, ensure_ascii=False),
                expires_at,
                now.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
