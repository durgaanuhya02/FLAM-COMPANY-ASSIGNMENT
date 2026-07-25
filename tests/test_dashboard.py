"""Bonus feature: the read-only web dashboard. Drives it as a real
subprocess over HTTP, the same way a browser would, using only stdlib
urllib (no extra dependency for tests either)."""

import json
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request

from tests.helpers import REPO_ROOT, Harness

PORT = 8765


def _get(path):
    with urllib.request.urlopen(f"http://127.0.0.1:{PORT}{path}", timeout=5) as resp:
        return resp.status, resp.read().decode("utf-8")


class TestDashboard(unittest.TestCase):
    def setUp(self):
        self.h = Harness()
        self.h.enqueue({"id": "j1", "command": "exit 0"})
        self.dash_proc = subprocess.Popen(
            [sys.executable, "-m", "queuectl", "dashboard", "--port", str(PORT)],
            cwd=REPO_ROOT,
            env=self.h.env,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                _get("/")
                break
            except (urllib.error.URLError, ConnectionError):
                time.sleep(0.2)
        else:
            self.fail("dashboard never came up")

    def tearDown(self):
        self.dash_proc.terminate()
        try:
            self.dash_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.dash_proc.kill()
        self.h.cleanup()

    def test_root_page_lists_the_job(self):
        status, body = _get("/")
        self.assertEqual(status, 200)
        self.assertIn("j1", body)
        self.assertIn("exit 0", body)

    def test_api_jobs_returns_json(self):
        status, body = _get("/api/jobs")
        self.assertEqual(status, 200)
        jobs = json.loads(body)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["id"], "j1")

    def test_api_jobs_state_filter(self):
        status, body = _get("/api/jobs?state=dead")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), [])

    def test_unknown_route_is_404(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            _get("/nope")
        self.assertEqual(ctx.exception.code, 404)


if __name__ == "__main__":
    unittest.main()
