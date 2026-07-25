"""Worker execution loop: claim a job, run its command, record the result."""

from __future__ import annotations

import os
import subprocess
import time

from . import db
from .claim import claim_next_job
from .models import Job, JobState, now_ts

POLL_INTERVAL_SECONDS = 1.0


def execute_job(conn, job: Job) -> None:
    """Run job.command via the shell and record success or failure.

    Retry/backoff scheduling is not decided here -- this just runs the
    command and records the raw outcome; scheduling policy lives with the
    caller so it can be unit-tested independently of subprocess execution.
    """
    result = subprocess.run(job.command, shell=True)
    success = result.returncode == 0

    now = now_ts()
    if success:
        conn.execute(
            "UPDATE jobs SET state = ?, updated_at = ?, last_error = NULL WHERE id = ?",
            (JobState.COMPLETED, now, job.id),
        )
    else:
        conn.execute(
            "UPDATE jobs SET state = ?, attempts = attempts + 1, updated_at = ?, "
            "last_error = ? WHERE id = ?",
            (JobState.FAILED, now, f"exit code {result.returncode}", job.id),
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
