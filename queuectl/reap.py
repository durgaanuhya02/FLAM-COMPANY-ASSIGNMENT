"""Crash recovery: reclaim jobs whose worker died mid-execution.

A job in `processing` state is considered abandoned once its lease --
the `updated_at` timestamp, renewed by a heartbeat while the job runs,
see worker.execute_job -- hasn't been touched for LEASE_TIMEOUT_SECONDS.
This is checked opportunistically by every worker at the top of its poll
loop, so recovery does not depend on the crashed process doing anything:
any other live worker, or a freshly restarted one, reclaims it.

Reclaiming reuses the normal failure path: attempts is incremented and
the job goes back to `failed` with a backoff delay (or `dead` if that
exhausts max_retries). Counting a crash as a failed attempt is deliberate
-- see DECISIONS.md -- so a job that reliably kills its own worker gets
sent to the DLQ instead of being reclaimed forever.
"""

from __future__ import annotations

import sqlite3

from .backoff import compute_delay
from .models import JobState, now_ts

LEASE_TIMEOUT_SECONDS = 20


def reap_expired_leases(
    conn: sqlite3.Connection, backoff_base: int, lease_timeout: float = LEASE_TIMEOUT_SECONDS
) -> int:
    now = now_ts()
    cutoff = now - lease_timeout

    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE state = ? AND updated_at < ?",
            (JobState.PROCESSING, cutoff),
        ).fetchall()

        for row in rows:
            attempts = row["attempts"] + 1
            last_error = "worker died mid-execution (lease expired)"

            if attempts >= row["max_retries"]:
                conn.execute(
                    "UPDATE jobs SET state = ?, attempts = ?, updated_at = ?, "
                    "next_retry_at = NULL, worker_pid = NULL, last_error = ? WHERE id = ?",
                    (JobState.DEAD, attempts, now, last_error, row["id"]),
                )
            else:
                next_retry_at = now + compute_delay(attempts, backoff_base)
                conn.execute(
                    "UPDATE jobs SET state = ?, attempts = ?, updated_at = ?, "
                    "next_retry_at = ?, worker_pid = NULL, last_error = ? WHERE id = ?",
                    (JobState.FAILED, attempts, now, next_retry_at, last_error, row["id"]),
                )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return len(rows)
