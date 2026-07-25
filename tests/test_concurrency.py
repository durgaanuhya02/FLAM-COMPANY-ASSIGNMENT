"""Scenario 3 from the assignment: many jobs across multiple real worker
OS processes, every job runs exactly once."""

import sys
import time
import unittest
from pathlib import Path

from tests.helpers import REPO_ROOT, Harness

JOB_COUNT = 20
WORKER_COUNT = 4
RECORD_SCRIPT = REPO_ROOT / "tests" / "_record.py"


class TestConcurrency(unittest.TestCase):
    def setUp(self):
        self.h = Harness()
        self.results_dir = Path(self.h.tmpdir) / "results"
        self.results_dir.mkdir()

    def tearDown(self):
        self.h.cleanup()

    def test_every_job_runs_exactly_once(self):
        for i in range(JOB_COUNT):
            out_file = self.results_dir / f"job{i}.out"
            command = f'"{sys.executable}" "{RECORD_SCRIPT}" "{out_file}"'
            self.h.enqueue({"id": f"job{i}", "command": command})

        worker_proc = self.h.start_worker(count=WORKER_COUNT)
        try:
            deadline = time.time() + 60
            while time.time() < deadline:
                jobs = self.h.list_jobs()
                if all(j["state"] == "completed" for j in jobs) and len(jobs) == JOB_COUNT:
                    break
                time.sleep(0.5)
            else:
                self.fail("jobs did not all complete within 60s")
        finally:
            self.h.stop_process_tree(worker_proc)

        jobs = self.h.list_jobs()
        self.assertEqual(len(jobs), JOB_COUNT)
        for j in jobs:
            self.assertEqual(j["state"], "completed")
            self.assertEqual(j["attempts"], 0)

        # The real duplicate-execution check: each job's own output file
        # must have exactly one line. Two lines would mean two workers
        # (or one worker twice) ran the same job.
        for i in range(JOB_COUNT):
            out_file = self.results_dir / f"job{i}.out"
            self.assertTrue(out_file.exists(), f"job{i} never ran")
            lines = out_file.read_text().strip().splitlines()
            self.assertEqual(len(lines), 1, f"job{i} ran {len(lines)} times: {lines}")


if __name__ == "__main__":
    unittest.main()
