"""SQLite connection and schema management.

Every queuectl process (CLI commands and workers) opens its own connection
to the same on-disk database file. SQLite serializes writers at the file
level, which is what makes cross-process job claiming safe -- see claim.py.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DEFAULT_DB_DIR = ".queuectl"
DEFAULT_DB_NAME = "queue.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    next_retry_at REAL,
    worker_pid INTEGER,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    pid INTEGER PRIMARY KEY,
    started_at REAL NOT NULL,
    last_seen REAL NOT NULL
);
"""


def db_path() -> Path:
    override = os.environ.get("QUEUECTL_DB")
    if override:
        return Path(override)
    return Path.cwd() / DEFAULT_DB_DIR / DEFAULT_DB_NAME


def connect(path: Path | None = None) -> sqlite3.Connection:
    path = path or db_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    # isolation_level=None => autocommit mode. We manage transactions
    # explicitly with BEGIN IMMEDIATE where atomicity matters (claim.py),
    # instead of relying on sqlite3's implicit transaction handling.
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(SCHEMA)
    return conn


DEFAULT_CONFIG = {
    "max-retries": "3",
    "backoff-base": "2",
}


def get_config(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    if row is not None:
        return row["value"]
    return DEFAULT_CONFIG[key]


def set_config(conn: sqlite3.Connection, key: str, value: str) -> None:
    if key not in DEFAULT_CONFIG:
        raise ValueError(f"unknown config key: {key!r} (expected one of {list(DEFAULT_CONFIG)})")
    conn.execute(
        "INSERT INTO config(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
