from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_DB_PATH = DATA_DIR / "cache_store.sqlite3"

load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.example", override=False)

try:
    import libsql  # type: ignore
except ImportError:  # pragma: no cover - optional dependency in local env
    libsql = None

def _env_text(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = str(value).strip()
    if normalized == "":
        return default
    return normalized


def _mask_secret(value: str | None, *, keep_prefix: int = 6, keep_suffix: int = 4) -> str | None:
    if not value:
        return None
    if len(value) <= keep_prefix + keep_suffix:
        return "*" * len(value)
    return f"{value[:keep_prefix]}...{value[-keep_suffix:]}"


def get_database_backend_snapshot() -> dict[str, Any]:
    url = _env_text("TURSO_DATABASE_URL")
    token = _env_text("TURSO_AUTH_TOKEN")
    turso_requested = bool(url and token)
    turso_active = turso_requested and libsql is not None
    return {
        "backend": "turso" if turso_active else "sqlite",
        "turso_mode": "remote" if turso_active else None,
        "turso_url_present": bool(url),
        "turso_auth_token_present": bool(token),
        "turso_url_masked": _mask_secret(url),
        "turso_auth_token_masked": _mask_secret(token),
        "libsql_installed": libsql is not None,
        "embedded_replica_enabled": False,
        "remote_connection_enabled": turso_active,
        "local_db_path": str(DEFAULT_DB_PATH),
    }


def _using_turso() -> bool:
    snapshot = get_database_backend_snapshot()
    return bool(snapshot["embedded_replica_enabled"])


def _connect_sqlite(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _connect_turso() -> Any:
    url = _env_text("TURSO_DATABASE_URL")
    token = _env_text("TURSO_AUTH_TOKEN")
    if libsql is None or not url or not token:
        return _connect_sqlite(DEFAULT_DB_PATH)

    conn = libsql.connect(url, auth_token=token)
    try:
        conn.row_factory = sqlite3.Row
    except Exception:
        pass
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    return conn


@contextmanager
def db_connection(db_path: Path | None = None) -> Iterator[Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target_path = db_path or DEFAULT_DB_PATH
    if _using_turso():
        conn = _connect_turso()
    else:
        conn = _connect_sqlite(target_path)
    initial_total_changes = 0
    try:
        initial_total_changes = int(getattr(conn, "total_changes", 0) or 0)
    except Exception:
        initial_total_changes = 0
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
