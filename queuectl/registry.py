"""Worker registry: lets `worker stop`, running in a different terminal
and a different process than the workers themselves, discover and signal
them.

Each worker process inserts a row for its own PID into the `workers`
table on startup and deletes it on clean exit. `worker stop` reads this
table and sends SIGTERM to every PID in it -- this is the "how does a
different terminal find live workers" design decision. Rejected
alternatives, and why, are in DECISIONS.md.
"""

from __future__ import annotations

import sqlite3
from typing import List

from .models import now_ts


def register(conn: sqlite3.Connection, pid: int) -> None:
    now = now_ts()
    conn.execute(
        "INSERT INTO workers (pid, started_at, last_seen) VALUES (?, ?, ?) "
        "ON CONFLICT(pid) DO UPDATE SET last_seen = excluded.last_seen",
        (pid, now, now),
    )


def unregister(conn: sqlite3.Connection, pid: int) -> None:
    conn.execute("DELETE FROM workers WHERE pid = ?", (pid,))


def list_pids(conn: sqlite3.Connection) -> List[int]:
    return [row["pid"] for row in conn.execute("SELECT pid FROM workers ORDER BY started_at")]
