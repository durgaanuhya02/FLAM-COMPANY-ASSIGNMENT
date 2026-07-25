# Design decisions

## 1. Which exact line(s) prevent two workers from claiming the same job, and why is that operation atomic across separate OS processes?

[`queuectl/claim.py`](queuectl/claim.py), the whole `claim_next_job` function, but the two lines that matter are:

```python
conn.execute("BEGIN IMMEDIATE")
...
row = conn.execute("SELECT * FROM jobs WHERE state = ? OR (state = ? AND next_retry_at <= ?) "
                    "ORDER BY created_at LIMIT 1", ...).fetchone()
...
conn.execute("UPDATE jobs SET state = ?, worker_pid = ?, updated_at = ? WHERE id = ?", ...)
conn.execute("COMMIT")
```

`BEGIN IMMEDIATE` acquires a RESERVED lock on the SQLite database **file** the
moment it runs, not just an in-process lock. SQLite's locking is
implemented at the OS file-lock level (`fcntl`/`LockFileEx` depending on
platform), so it applies uniformly to every connection from every
process that opens that file, anywhere. Once one process holds that
lock, every other process's own `BEGIN IMMEDIATE` blocks (up to
`busy_timeout`, set to 30s in `db.py`) until the first transaction
commits or rolls back.

That means the SELECT and the UPDATE together are indivisible from any
other process's point of view: no other worker can read the row as
`pending` and also decide to claim it, because the exclusive write lock
was already held before we even ran the SELECT. `worker_pid` is set in
the same UPDATE, so by the time the lock is released the row unambiguously
belongs to one worker. I verified this holds under real concurrency, not
just in theory: `tests/test_concurrency.py` runs 20 jobs across 4 real
worker OS processes (`multiprocessing.Process`, not threads) and asserts
every job's own output file has exactly one line, i.e. ran exactly once.

I deliberately did *not* use a two-step "SELECT then UPDATE ... WHERE
state='pending'" pattern relying on the UPDATE's affected-row-count to
detect a lost race. That would also be correct with SQLite (the UPDATE
itself is still serialized), but wrapping both in one explicit
transaction makes the atomicity argument a single, inspectable unit
instead of something implied by two separate statements.

## 2. A worker is SIGKILLed halfway through a job. Walk through, step by step, what state the job is in and how it eventually runs again. What is the worst-case delay before recovery?

Step by step:

1. Worker claims the job: `claim.py` sets `state='processing'`,
   `worker_pid=<pid>`, `updated_at=now`.
2. While the job's command runs, `worker.execute_job` doesn't block on a
   single `subprocess.run()` -- it uses `Popen` + a `wait(timeout=5)`
   loop (`HEARTBEAT_INTERVAL_SECONDS`), and on every timeout it re-runs
   `UPDATE jobs SET updated_at=now WHERE id=? AND state='processing'`.
   `updated_at` is the job's **lease**: proof that a live worker is still
   watching it.
3. `SIGKILL` arrives. It cannot be caught -- the process is gone
   instantly, mid-`wait()`. No handler runs, `updated_at` is never
   touched again. The row is now permanently stuck at
   `state='processing'` as far as the database is concerned, with a
   `worker_pid` that no longer refers to a live process.
4. Nothing about the crash itself changes the row. Recovery is entirely
   the responsibility of *other* workers: every worker, at the top of
   every poll iteration (`worker.run_forever`, before it tries to claim
   new work), calls `reap.reap_expired_leases(conn, backoff_base)`.
5. That function (also wrapped in its own `BEGIN IMMEDIATE` transaction,
   for the same cross-process-atomicity reason as claiming) finds every
   `processing` row whose `updated_at` is older than
   `LEASE_TIMEOUT_SECONDS` (20s) and reclaims it: `attempts` is
   incremented, `worker_pid` is cleared, and it goes back to `failed`
   with a normal backoff delay (or `dead` if that exhausts
   `max_retries`) -- the exact same state transition a real command
   failure would produce.
6. Once due, `claim_next_job` picks it up again (a `failed` job with
   `next_retry_at <= now` is directly claimable, no separate "revive to
   pending" step), and some worker -- possibly a different one, possibly
   a freshly restarted one -- runs it.

Worst case, with defaults: `LEASE_TIMEOUT_SECONDS` (20s) + one poll
interval to notice it (`POLL_INTERVAL_SECONDS`, 1s) + first backoff delay
(`2^1` = 2s) + one more poll interval to reclaim-and-run it (1s) ≈ **24s**
before the job is running again, comfortably inside the 60s requirement.
I verified this figure empirically, not just on paper: a controlled
kill-and-restart run (`tests/test_crash_recovery.py`, and an earlier
manual harness during development) measured ~20-21s to reclaim and ~24s
to full completion of a 3s job.

**Trade-off I'm accepting:** if the entire fleet of workers is killed
(not just one), nothing reaps anything until a worker is started again --
there's no standalone daemon. I judged a dedicated reaper process not
worth the extra moving part for this scope, since the same poll loop
that claims work is a natural, already-running place to also reap;
"restart your workers" is an acceptable operational answer here, and the
20s-since-`updated_at` check fires immediately on the very next loop
iteration regardless of *when* that restart happens.

**A second, more fundamental trade-off:** `SIGKILL` cannot be caught, so
nothing in the worker process can kill its own child (the job's
subprocess) in response. On POSIX, if only the worker's PID is killed
(not its process group), the in-flight command can become an orphan and
keep running to completion independently, while the job is *also*
reclaimed and re-run by another worker -- i.e. the command may genuinely
execute more than once for a crash specifically (not for the atomic
*claiming* path, which is unaffected). Properly closing this gap means
starting each job in its own process group and having whatever kills the
worker also kill that group (`setsid` + `killpg`), which is an operations
concern outside a single worker's own control and out of scope here. I'm
naming it rather than pretending it isn't a real edge case.

## 3. Does `dlq retry` reset `attempts`? Why is that the right call?

Yes -- see `cmd_dlq_retry` in `queuectl/cli.py`: `attempts` is set to `0`
and `next_retry_at` is cleared.

A job only reaches the DLQ after burning its entire automatic retry
budget on its own. A human running `dlq retry` is a distinct, deliberate
signal: they've looked at it (probably via `dlq list`, possibly
`last_error`) and decided it deserves another shot -- most commonly
because they believe they've fixed whatever caused it to keep failing
(a downstream dependency was down, a bad argument in the command,
etc.). Treating that as "just one more automatic attempt" (i.e. leaving
`attempts` at `max_retries` and immediately re-dying) would make manual
retry pointless. Resetting to 0 gives it the same full retry budget as a
brand-new job, which matches what a human actually means by "try this
again."

The cost of that choice: if the human is wrong and the job is still
fundamentally broken, it burns through the full retry budget a second
time before dying again, rather than dying immediately. I think that's
the right failure mode to prefer -- optimizing for "don't waste the
operator's fix" over "fail fast on a second attempt."

## 4. What designs did you consider and reject for `worker stop` (cross-process signaling), and why?

Chosen: a `workers` table in the same SQLite database
(`queuectl/registry.py`). Every worker process inserts its own PID on
start and deletes it on clean exit; `worker stop`, run from any other
terminal, reads that table and sends `SIGTERM` to each PID
(`cmd_worker_stop` in `cli.py`), then polls the table until it empties or
a timeout is hit.

Rejected:

- **A Unix domain socket / small control server per worker.** Each
  worker would listen for a "stop" message instead of being polled via
  the DB. More precise (an explicit request/ack instead of "send a
  signal and watch a table"), but it means every worker also has to run
  a second concurrent responsibility (accepting connections) alongside
  its job loop, plus socket-path lifecycle/cleanup, plus it doesn't
  generalize to non-POSIX transports for free. The DB is already the one
  thing every process in this system agrees to share and already polls;
  reusing it means "how does another terminal find a worker" reduces to
  a single indexed table instead of an entirely separate IPC mechanism.
- **`pkill`/`pgrep` matching on the worker's command line.** No new code
  at all, but it's fragile: it can't distinguish "our" workers from any
  other process that happens to share enough of the command line
  (another project's workers, a copy-pasted shell history entry, a
  developer's own ad hoc test run), and it silently does nothing useful
  if the invocation string changes (a wrapper script, an alias, a
  different working directory). A registry keyed by real PIDs the
  workers themselves inserted has no ambiguity about which processes are
  actually ours.
- **A single PID file per worker under a known directory** (e.g.
  `.queuectl/workers/<pid>.pid`), instead of a DB table. Functionally
  almost identical to what I built, and would work fine. I went with a
  table instead purely because the database already has WAL mode,
  `busy_timeout`, and the same locking guarantees I rely on elsewhere,
  so `worker stop` doesn't need to invent its own directory-scanning and
  stale-file cleanup logic -- a `DELETE`/stale-row check on kill failure
  (`cmd_worker_stop`'s `except (ProcessLookupError, OSError)` branch)
  does the same job with code I already trust.

## 5. If priorities were added tomorrow (high-priority jobs jump the queue), which parts of your design survive unchanged and which break?

**Survives unchanged:**

- The atomicity mechanism itself (`BEGIN IMMEDIATE` in `claim.py` and
  `reap.py`) -- it has nothing to do with *which* job gets picked, only
  that picking-and-marking is indivisible. A priority-aware claim query
  is exactly as atomic as today's.
- Retry/backoff, the DLQ, `dlq retry`'s attempts-reset behavior, the
  lease/heartbeat crash-recovery mechanism, graceful shutdown, and
  `worker stop`'s registry -- none of them look at ordering at all.
- The job schema is additive: a new `priority INTEGER NOT NULL DEFAULT 0`
  column doesn't touch any existing row's meaning.
- `config` as a mechanism (a key/value table) -- would just gain a new
  key if priority levels needed to be configurable (e.g. how many
  priority bands exist).

**Breaks / needs real changes:**

- The claim query's `ORDER BY created_at LIMIT 1` in `claim.py` becomes
  `ORDER BY priority DESC, created_at LIMIT 1` (or similar) -- this is a
  one-line change, but it's the one place true FIFO ordering was an
  explicit assumption, and anything that assumed "oldest job always runs
  next" (nothing currently does, but a naive test might) would need
  re-checking.
- `enqueue` needs a `--priority`/JSON field, `list` probably wants a
  `--priority` filter or to show it in output, and `status` might want
  a per-priority breakdown for operational visibility.
- Starvation becomes a real risk that doesn't exist today: a steady
  stream of high-priority jobs could indefinitely delay a low-priority
  one. Today's FIFO queue has no such failure mode. I'd want either an
  aging rule (priority effectively increases with wait time) or a
  reserved-capacity policy (e.g. one of N workers always pulls oldest
  regardless of priority) -- neither exists today and both are genuinely
  new design surface, not a tweak.
