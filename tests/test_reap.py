"""Direct tests of the lease-expiry reclaim logic, independent of OS
signal delivery (see test_crash_recovery.py for the POSIX SIGKILL
end-to-end version). These run on every platform."""

import shutil
import tempfile
import unittest
from pathlib import Path

from queuectl import db
from queuectl.models import JobState, now_ts
from queuectl.reap import reap_expired_leases


class TestReap(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="queuectl_reap_test_")
        self.conn = db.connect(Path(self.tmpdir) / "queue.db")

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _insert_processing(self, job_id, updated_at, attempts=0, max_retries=3):
        self.conn.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, worker_pid) "
            "VALUES (?, 'true', 'processing', ?, ?, ?, ?, 99999)",
            (job_id, attempts, max_retries, updated_at, updated_at),
        )

    def test_fresh_lease_is_left_alone(self):
        self._insert_processing("a", updated_at=now_ts())
        reaped = reap_expired_leases(self.conn, backoff_base=2, lease_timeout=20)
        self.assertEqual(reaped, 0)
        row = self.conn.execute("SELECT state FROM jobs WHERE id='a'").fetchone()
        self.assertEqual(row["state"], JobState.PROCESSING)

    def test_expired_lease_with_retries_left_goes_to_failed_with_backoff(self):
        self._insert_processing("a", updated_at=now_ts() - 100, attempts=0, max_retries=3)
        reaped = reap_expired_leases(self.conn, backoff_base=2, lease_timeout=20)
        self.assertEqual(reaped, 1)
        row = self.conn.execute("SELECT * FROM jobs WHERE id='a'").fetchone()
        self.assertEqual(row["state"], JobState.FAILED)
        self.assertEqual(row["attempts"], 1)
        self.assertIsNone(row["worker_pid"], "worker_pid must be cleared on reclaim")
        self.assertGreater(row["next_retry_at"], now_ts())

    def test_expired_lease_that_exhausts_retries_goes_dead(self):
        self._insert_processing("a", updated_at=now_ts() - 100, attempts=2, max_retries=3)
        reap_expired_leases(self.conn, backoff_base=2, lease_timeout=20)
        row = self.conn.execute("SELECT * FROM jobs WHERE id='a'").fetchone()
        self.assertEqual(row["state"], JobState.DEAD)
        self.assertEqual(row["attempts"], 3)

    def test_reap_is_idempotent_once_reclaimed(self):
        self._insert_processing("a", updated_at=now_ts() - 100)
        reap_expired_leases(self.conn, backoff_base=2, lease_timeout=20)
        # second pass should find nothing left in 'processing' to reclaim
        reaped_again = reap_expired_leases(self.conn, backoff_base=2, lease_timeout=20)
        self.assertEqual(reaped_again, 0)


if __name__ == "__main__":
    unittest.main()
