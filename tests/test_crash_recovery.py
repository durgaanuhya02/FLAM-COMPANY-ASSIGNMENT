"""Scenario 4 from the assignment: a worker is SIGKILLed mid-job; after
restart, the job still completes and nothing is stuck in 'processing'.
Worst-case recovery must be under 60s.

SIGKILL cannot be caught by definition, so this can only be exercised
where the OS actually delivers real signals -- POSIX (Linux/macOS/WSL).
On Windows, os.kill() falls back to TerminateProcess regardless of the
signal requested, which was confirmed manually during development; the
underlying reclaim logic is covered instead by test_reap.py, which
doesn't depend on OS signal delivery.
"""

import os
import signal
import sys
import time
import unittest

from tests.helpers import Harness

POSIX_SIGKILL = os.name == "posix" and hasattr(signal, "SIGKILL")


@unittest.skipUnless(POSIX_SIGKILL, "real SIGKILL delivery requires POSIX")
class TestCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def tearDown(self):
        self.h.cleanup()

    def test_sigkilled_worker_job_is_recovered_and_completes(self):
        self.h.enqueue({"id": "crashy", "command": "sleep 5", "max_retries": 3})

        worker_proc = self.h.start_worker(count=1)
        claimed_pid = None
        deadline = time.time() + 10
        while time.time() < deadline:
            jobs = self.h.list_jobs()
            if jobs and jobs[0]["state"] == "processing":
                claimed_pid = jobs[0]["worker_pid"]
                break
            time.sleep(0.2)
        self.assertIsNotNone(claimed_pid, "job never entered processing")

        t_kill = time.time()
        os.kill(claimed_pid, signal.SIGKILL)
        self.h.stop_process_tree(worker_proc)  # reap the (now-defunct) supervisor

        # Confirm nothing "fixed itself" instantly -- it should still show
        # processing, owned by the now-dead pid, until the lease expires.
        row = self.h.list_jobs()[0]
        self.assertEqual(row["state"], "processing")
        self.assertEqual(row["worker_pid"], claimed_pid)

        recovery_proc = self.h.start_worker(count=1)
        try:
            deadline = t_kill + 60
            final = None
            while time.time() < deadline:
                row = self.h.list_jobs()[0]
                if row["state"] == "completed":
                    final = row
                    break
                time.sleep(0.5)
        finally:
            self.h.stop_process_tree(recovery_proc)

        elapsed = time.time() - t_kill
        self.assertIsNotNone(final, f"job never recovered within 60s (waited {elapsed:.1f}s)")
        self.assertLess(elapsed, 60, "recovery exceeded the 60s worst-case bound")
        self.assertEqual(final["attempts"], 1, "the crash should count as one failed attempt")
        self.assertNotEqual(final["worker_pid"], claimed_pid, "should have been re-run by a different worker")


if __name__ == "__main__":
    unittest.main()
