# queuectl

A CLI-based background job queue: worker processes, automatic retries
with exponential backoff, and a Dead Letter Queue (DLQ) for jobs that
exhaust their retries. Built for the QueueCTL backend internship
assignment.

Design rationale and trade-offs (the "why", not the "what") live in
[DECISIONS.md](DECISIONS.md).

## Requirements

- Python 3.9+ (standard library only -- no `pip install` needed)
- bash
- POSIX (Linux/macOS/WSL). Worker crash recovery relies on `SIGKILL`
  being uncatchable and `worker stop` relies on real cross-process
  `SIGTERM` delivery -- neither exists on native Windows. Use WSL if
  you're on Windows.

## Setup

```bash
git clone https://github.com/durgaanuhya02/FLAM-COMPANY-ASSIGNMENT.git
cd FLAM-COMPANY-ASSIGNMENT/queuectl
export PATH="$PWD/bin:$PATH"
queuectl --help
```

`bin/queuectl` is a thin wrapper around `python3 -m queuectl`; there's
nothing to build or install.

## Usage

```bash
# add jobs (id is optional -- a uuid is generated if omitted)
queuectl enqueue '{"id":"job1","command":"echo hello"}'
queuectl enqueue '{"command":"sleep 2","max_retries":5}'

# start 3 workers in the foreground (blocks until stopped)
queuectl worker start --count 3

# ...from another terminal:
queuectl status
queuectl list --state pending
queuectl list --state completed --json

# stop all running workers, from any terminal
queuectl worker stop

# a job that exhausted its retries lands in the DLQ
queuectl dlq list
queuectl dlq retry job1

# retry count / backoff base
queuectl config get max-retries
queuectl config set max-retries 5
queuectl config set backoff-base 3
```

The database lives at `.queuectl/queue.db`, relative to the current
working directory (override with `QUEUECTL_DB=/path/to/queue.db`). Run
`queuectl` commands from the same directory each time so they all see
the same queue.

## Architecture

Everything is one SQLite file (`.queuectl/queue.db`) plus stateless CLI
processes that all open it. There is no long-running server process
except the worker loops themselves.

```
queuectl/
  cli.py       argparse dispatch for every subcommand
  models.py    Job dataclass, JobState constants
  db.py        connection + schema + config table
  claim.py     atomic cross-process job claiming (BEGIN IMMEDIATE)
  worker.py    the poll/execute loop, one per OS process
  backoff.py   delay = base ** attempts
  reap.py      reclaims jobs abandoned by a crashed worker
  registry.py  workers table, for cross-terminal `worker stop`
```

- **Job lifecycle**: `pending` → `processing` → `completed`, or
  `processing` → `failed` (retryable, waiting on `next_retry_at`) →
  `dead` (DLQ) once `max_retries` is exhausted. A `failed` job past its
  `next_retry_at` is directly claimable -- there's no separate
  "requeue to pending" step.
- **Claiming** (`claim.py`): one `BEGIN IMMEDIATE` transaction does
  "pick the oldest claimable job" and "mark it `processing`" as a single
  atomic unit, relying on SQLite serializing writers at the file level
  across every process. See DECISIONS.md Q1.
- **Crash recovery** (`reap.py`, `worker.py`): a `processing` job's
  `updated_at` is a lease, renewed every 5s while its command runs.
  Every worker's poll loop reclaims any job whose lease has gone silent
  for 20s+, regardless of which worker originally claimed it. See
  DECISIONS.md Q2 for the full walkthrough and worst-case timing.
- **Graceful shutdown**: `SIGTERM`/`SIGINT` set a flag checked only
  *before* claiming the next job -- an in-flight job always finishes.
  `SIGKILL` bypasses this entirely by design; that's what the reaper is
  for.
- **`worker stop` across terminals** (`registry.py`): workers register
  their PID in a `workers` table on start and remove it on clean exit;
  `worker stop` signals every PID it finds there. See DECISIONS.md Q4
  for rejected alternatives.

## Testing

```bash
python3 -m unittest discover -s tests -t . -v
```

- `test_backoff.py` -- the `base ** attempts` formula
- `test_claim.py` -- claim ordering, state filtering, and that a second
  connection sees a claim immediately after commit
- `test_reap.py` -- lease-expiry reclaim logic directly (cross-platform)
- `test_concurrency.py` -- **scenario 3**: 20 jobs across 4 real worker
  OS processes, asserts every job's output was written exactly once
- `test_dlq.py` -- **scenario 2**: exhausted retries land in the DLQ,
  `dlq retry` resets `attempts`
- `test_crash_recovery.py` -- **scenario 4**: real `SIGKILL` mid-job,
  asserts recovery completes within the 60s bound (POSIX only --
  skipped on Windows, since `os.kill()` there can't deliver a real
  signal; confirmed manually during development instead)
- `test_persistence.py` -- **scenario 5**: state survives a full restart
  with no worker process alive in between

Scenario 1 (a basic job completes) is exercised implicitly by every
other test that runs a worker.

## Demo recording

TODO: link here once recorded.
