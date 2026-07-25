"""Atomic cross-process job claiming.

Two workers running as separate OS processes must never claim the same
job. This relies entirely on SQLite's own writer serialization: whichever
process's connection issues `BEGIN IMMEDIATE` first acquires a RESERVED
lock on the *database file itself* (not an in-process lock), and every
other connection's `BEGIN IMMEDIATE` -- in any process, on any machine
talking to this file -- blocks until that lock is released by COMMIT or
ROLLBACK (up to `busy_timeout`, see db.py).

That means the SELECT (pick a candidate job) and the UPDATE (mark it
`processing`) below execute as one indivisible step from every other
worker's point of view: no other process can ever see the row as still
`pending`/`failed` and claim it too, because the write lock was already
held before we even read it.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from .models import Job, JobState, now_ts


def claim_next_job(conn: sqlite3.Connection, worker_pid: int) -> Optional[Job]:
    now = now_ts()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM jobs "
            "WHERE state = ? OR (state = ? AND next_retry_at <= ?) "
            "ORDER BY created_at LIMIT 1",
            (JobState.PENDING, JobState.FAILED, now),
        ).fetchone()

        if row is None:
            conn.execute("COMMIT")
            return None

        conn.execute(
            "UPDATE jobs SET state = ?, worker_pid = ?, updated_at = ? WHERE id = ?",
            (JobState.PROCESSING, worker_pid, now, row["id"]),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    job = Job.from_row(row)
    job.state = JobState.PROCESSING
    job.worker_pid = worker_pid
    job.updated_at = now
    return job
