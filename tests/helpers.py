"""Shared test utilities: an isolated temp DB per test and thin wrappers
for driving the real CLI/worker as subprocesses, the same way the
automated grading script and a human operator would."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


class Harness:
    def __init__(self):
        self.tmpdir = tempfile.mkdtemp(prefix="queuectl_test_")
        self.env = dict(os.environ)
        self.env["PYTHONPATH"] = str(REPO_ROOT)
        self.env["QUEUECTL_DB"] = str(Path(self.tmpdir) / "queue.db")

    def cleanup(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def cli(self, *args, timeout=15):
        return subprocess.run(
            [sys.executable, "-m", "queuectl", *args],
            cwd=REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def enqueue(self, job: dict):
        r = self.cli("enqueue", json.dumps(job))
        assert r.returncode == 0, r.stderr
        return r

    def list_jobs(self, state: str | None = None):
        args = ["list", "--json"]
        if state:
            args = ["list", "--state", state, "--json"]
        r = self.cli(*args)
        assert r.returncode == 0, r.stderr
        return json.loads(r.stdout)

    def start_worker(self, count: int = 1) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-m", "queuectl", "worker", "start", "--count", str(count)],
            cwd=REPO_ROOT,
            env=self.env,
        )

    def stop_process_tree(self, proc: subprocess.Popen):
        # Prefer the real product path: `worker stop` signals every
        # *registered* worker PID directly (the actual job-executing
        # children, not just the supervisor). Killing only `proc` itself
        # would leave its multiprocessing children orphaned and immortal.
        try:
            self.cli("worker", "stop", "--timeout", "10", timeout=15)
        except Exception:
            pass

        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
