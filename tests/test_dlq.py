"""Scenario 2 from the assignment: a failing job retries with backoff and
lands in the DLQ, and can be retried back out of it."""

import time
import unittest

from tests.helpers import Harness


class TestDlq(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def tearDown(self):
        self.h.cleanup()

    def test_exhausted_retries_land_in_dlq_then_can_be_retried(self):
        self.h.enqueue({"id": "always-fails", "command": "exit 1", "max_retries": 2})

        worker_proc = self.h.start_worker(count=1)
        try:
            deadline = time.time() + 15
            while time.time() < deadline:
                jobs = self.h.list_jobs()
                if jobs and jobs[0]["state"] == "dead":
                    break
                time.sleep(0.5)
            else:
                self.fail("job never reached the DLQ")
        finally:
            self.h.stop_process_tree(worker_proc)

        dead = self.h.list_jobs(state="dead")
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0]["attempts"], 2)

        r = self.h.cli("dlq", "retry", "always-fails")
        self.assertEqual(r.returncode, 0, r.stderr)

        pending = self.h.list_jobs(state="pending")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["attempts"], 0, "dlq retry must reset attempts")

    def test_dlq_retry_rejects_a_job_that_is_not_dead(self):
        self.h.enqueue({"id": "j1", "command": "true"})
        r = self.h.cli("dlq", "retry", "j1")
        self.assertNotEqual(r.returncode, 0)

    def test_dlq_retry_rejects_unknown_job(self):
        r = self.h.cli("dlq", "retry", "no-such-job")
        self.assertNotEqual(r.returncode, 0)


if __name__ == "__main__":
    unittest.main()
