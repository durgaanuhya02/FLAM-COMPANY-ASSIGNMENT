"""Worker execution loop: claim a job, run its command, record the result."""

from __future__ import annotations

import os
import subprocess
import time

from . import db
from .backoff import compute_delay
from .claim import claim_next_job
from .models import Job, JobState, now_ts

POLL_INTERVAL_SECONDS = 1.0


def execute_job(conn, job: Job) -> None:
    """Run job.command via the shell and record the outcome.

    On failure: if the job still has retry budget left (attempts after
    this failure < max_retries), it goes back to `failed` with
    next_retry_at set by the backoff formula -- claim_next_job() already
    treats a due `failed` job as claimable, so no separate "requeue to
    pending" step is needed. Once attempts reaches max_retries, it moves
    to `dead` (the DLQ) permanently.
    """
    result = subprocess.run(job.command, shell=True)
    success = result.returncode == 0
    now = now_ts()

    if success:
        conn.execute(
            "UPDATE jobs SET state = ?, updated_at = ?, last_error = NULL WHERE id = ?",
            (JobState.COMPLETED, now, job.id),
        )
        return

    attempts = job.attempts + 1
    last_error = f"exit code {result.returncode}"

    if attempts >= job.max_retries:
        conn.execute(
            "UPDATE jobs SET state = ?, attempts = ?, updated_at = ?, next_retry_at = NULL, "
            "last_error = ? WHERE id = ?",
            (JobState.DEAD, attempts, now, last_error, job.id),
        )
        return

    backoff_base = int(db.get_config(conn, "backoff-base"))
    next_retry_at = now + compute_delay(attempts, backoff_base)
    conn.execute(
        "UPDATE jobs SET state = ?, attempts = ?, updated_at = ?, next_retry_at = ?, "
        "last_error = ? WHERE id = ?",
        (JobState.FAILED, attempts, now, next_retry_at, last_error, job.id),
    )


def run_forever(worker_index: int) -> None:
    """Main loop for a single worker OS process. Blocks forever, polling
    for claimable jobs. Never returns on its own -- the process is
    expected to be terminated externally (worker stop / SIGTERM / SIGKILL)."""
    conn = db.connect()
    pid = os.getpid()
    while True:
        job = claim_next_job(conn, pid)
        if job is None:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        execute_job(conn, job)
