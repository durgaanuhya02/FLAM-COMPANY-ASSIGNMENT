"""Worker execution loop: claim a job, run its command, record the result."""

from __future__ import annotations

import os
import subprocess
import time

from . import db
from .backoff import compute_delay
from .claim import claim_next_job
from .models import Job, JobState, now_ts
from .reap import reap_expired_leases

POLL_INTERVAL_SECONDS = 1.0
HEARTBEAT_INTERVAL_SECONDS = 5  # well under reap.LEASE_TIMEOUT_SECONDS (20s)


def execute_job(conn, job: Job) -> None:
    """Run job.command via the shell and record the outcome.

    Uses Popen + a bounded wait() loop instead of subprocess.run() so we
    can renew the job's lease (its `updated_at`) every
    HEARTBEAT_INTERVAL_SECONDS while a long-running command is still
    executing -- otherwise the reaper in reap.py would eventually mistake
    a slow-but-alive job for a crashed one.

    On failure: if the job still has retry budget left (attempts after
    this failure < max_retries), it goes back to `failed` with
    next_retry_at set by the backoff formula -- claim_next_job() already
    treats a due `failed` job as claimable, so no separate "requeue to
    pending" step is needed. Once attempts reaches max_retries, it moves
    to `dead` (the DLQ) permanently.
    """
    proc = subprocess.Popen(job.command, shell=True)
    while True:
        try:
            returncode = proc.wait(timeout=HEARTBEAT_INTERVAL_SECONDS)
            break
        except subprocess.TimeoutExpired:
            conn.execute(
                "UPDATE jobs SET updated_at = ? WHERE id = ? AND state = ?",
                (now_ts(), job.id, JobState.PROCESSING),
            )

    success = returncode == 0
    now = now_ts()

    if success:
        conn.execute(
            "UPDATE jobs SET state = ?, updated_at = ?, last_error = NULL WHERE id = ?",
            (JobState.COMPLETED, now, job.id),
        )
        return

    attempts = job.attempts + 1
    last_error = f"exit code {returncode}"

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
    expected to be terminated externally (worker stop / SIGTERM / SIGKILL).

    Every iteration also checks for jobs abandoned by a crashed worker
    (reap_expired_leases) before trying to claim new work -- any worker,
    not just the one that originally claimed a job, can recover it.
    """
    conn = db.connect()
    pid = os.getpid()
    while True:
        backoff_base = int(db.get_config(conn, "backoff-base"))
        reap_expired_leases(conn, backoff_base)

        job = claim_next_job(conn, pid)
        if job is None:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        execute_job(conn, job)
