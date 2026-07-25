import shutil
import tempfile
import time
import unittest
from pathlib import Path

from queuectl import db
from queuectl.claim import claim_next_job
from queuectl.models import JobState


class TestClaim(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="queuectl_claim_test_")
        self.conn = db.connect(Path(self.tmpdir) / "queue.db")

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _insert(self, job_id, state="pending", created_at=0.0, next_retry_at=None, max_retries=3):
        self.conn.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_retry_at) "
            "VALUES (?, 'true', ?, 0, ?, ?, ?, ?)",
            (job_id, state, max_retries, created_at, created_at, next_retry_at),
        )

    def test_claims_oldest_pending_first(self):
        self._insert("b", created_at=2)
        self._insert("a", created_at=1)
        job = claim_next_job(self.conn, worker_pid=111)
        self.assertEqual(job.id, "a")
        self.assertEqual(job.state, JobState.PROCESSING)
        self.assertEqual(job.worker_pid, 111)

    def test_processing_job_is_not_reclaimed(self):
        self._insert("a", state=JobState.PROCESSING, created_at=1)
        self.assertIsNone(claim_next_job(self.conn, worker_pid=111))

    def test_completed_and_dead_jobs_are_not_claimable(self):
        self._insert("a", state=JobState.COMPLETED, created_at=1)
        self._insert("b", state=JobState.DEAD, created_at=2)
        self.assertIsNone(claim_next_job(self.conn, worker_pid=111))

    def test_failed_job_not_claimable_before_its_backoff_delay(self):
        self._insert("a", state=JobState.FAILED, created_at=1, next_retry_at=time.time() + 100)
        self.assertIsNone(claim_next_job(self.conn, worker_pid=111))

    def test_failed_job_claimable_once_due(self):
        self._insert("a", state=JobState.FAILED, created_at=1, next_retry_at=time.time() - 1)
        job = claim_next_job(self.conn, worker_pid=111)
        self.assertEqual(job.id, "a")

    def test_claim_is_visible_to_a_second_connection_immediately(self):
        # Simulates a second OS process: a fresh connection to the same
        # file must see the row as already 'processing' right after the
        # first connection's claim commits.
        self._insert("a", created_at=1)
        claim_next_job(self.conn, worker_pid=111)

        other_conn = db.connect(Path(self.tmpdir) / "queue.db")
        try:
            self.assertIsNone(claim_next_job(other_conn, worker_pid=222))
            row = other_conn.execute("SELECT state, worker_pid FROM jobs WHERE id='a'").fetchone()
            self.assertEqual(row["state"], JobState.PROCESSING)
            self.assertEqual(row["worker_pid"], 111)
        finally:
            other_conn.close()


if __name__ == "__main__":
    unittest.main()
