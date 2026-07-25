"""Helper invoked as a job command by test_concurrency.py: append a line
to a per-job file so the test can detect whether a job ran more than
once (duplicate execution would show up as more than one line)."""

import os
import sys
import time

path = sys.argv[1]
with open(path, "a") as f:
    f.write(f"ran pid={os.getpid()} t={time.time()}\n")
