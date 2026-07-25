"""Scenario 5 from the assignment: jobs survive a full restart (no
worker process alive at all in between)."""

import time
import unittest

from tests.helpers import Harness


class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def tearDown(self):
        self.h.cleanup()

    def test_jobs_survive_a_full_restart(self):
        self.h.enqueue({"id": "j1", "command": "exit 0"})
        self.h.enqueue({"id": "j2", "command": "exit 0"})

        # No worker has ever run yet -- this is process restart #1: a
        # completely fresh CLI invocation reading back what enqueue wrote.
        pending = self.h.list_jobs(state="pending")
        self.assertEqual({j["id"] for j in pending}, {"j1", "j2"})

        worker_proc = self.h.start_worker(count=1)
        try:
            deadline = time.time() + 15
            while time.time() < deadline:
                jobs = self.h.list_jobs()
                if all(j["state"] == "completed" for j in jobs):
                    break
                time.sleep(0.5)
            else:
                self.fail("jobs never completed")
        finally:
            self.h.stop_process_tree(worker_proc)

        # Worker process (and the CLI process that started it) is now
        # fully gone. A brand new CLI invocation must still see the
        # completed jobs -- this is what "persistent storage across
        # restarts" means: state lives in the DB file, not in memory.
        completed = self.h.list_jobs(state="completed")
        self.assertEqual({j["id"] for j in completed}, {"j1", "j2"})


if __name__ == "__main__":
    unittest.main()
