"""Argument parsing and command dispatch for the queuectl CLI."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import signal
import sqlite3
import sys
import time
import uuid
from typing import Sequence

from . import db, registry, webui, worker
from .models import Job, JobState, now_ts


def cmd_enqueue(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.job_json)
    except json.JSONDecodeError as e:
        print(f"error: invalid JSON: {e}", file=sys.stderr)
        return 1

    if "command" not in payload or not payload["command"]:
        print("error: job JSON must include a non-empty \"command\"", file=sys.stderr)
        return 1

    conn = db.connect()
    job_id = payload.get("id") or str(uuid.uuid4())
    default_max_retries = int(db.get_config(conn, "max-retries"))
    ts = now_ts()

    job = Job(
        id=job_id,
        command=payload["command"],
        state=JobState.PENDING,
        attempts=int(payload.get("attempts", 0)),
        max_retries=int(payload.get("max_retries", default_max_retries)),
        created_at=ts,
        updated_at=ts,
    )

    try:
        conn.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (job.id, job.command, job.state, job.attempts, job.max_retries, job.created_at, job.updated_at),
        )
    except sqlite3.IntegrityError:
        print(f"error: a job with id {job.id!r} already exists", file=sys.stderr)
        return 1

    print(f"enqueued {job.id}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    conn = db.connect()
    if args.state:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY created_at", (args.state,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()

    jobs = [Job.from_row(r) for r in rows]

    if args.json:
        # Interface contract: JSON array on stdout, nothing else on stdout.
        print(json.dumps([j.to_dict() for j in jobs]))
        return 0

    if not jobs:
        print("no jobs")
        return 0

    for j in jobs:
        print(f"{j.id}\t{j.state}\t{j.attempts}/{j.max_retries}\t{j.command}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    conn = db.connect()
    counts = dict.fromkeys(JobState.ALL, 0)
    for row in conn.execute("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state"):
        counts[row["state"]] = row["n"]

    active_workers = conn.execute("SELECT COUNT(*) AS n FROM workers").fetchone()["n"]

    print("jobs:")
    for state in JobState.ALL:
        print(f"  {state:<10} {counts[state]}")
    print(f"active workers: {active_workers}")
    return 0


def cmd_dlq_list(args: argparse.Namespace) -> int:
    args.state = JobState.DEAD
    return cmd_list(args)


def cmd_dlq_retry(args: argparse.Namespace) -> int:
    conn = db.connect()
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (args.job_id,)).fetchone()
    if row is None:
        print(f"error: no such job: {args.job_id!r}", file=sys.stderr)
        return 1
    if row["state"] != JobState.DEAD:
        print(f"error: job {args.job_id!r} is not in the DLQ (state={row['state']!r})", file=sys.stderr)
        return 1

    # Resetting attempts to 0: a DLQ retry is a human deciding the job
    # deserves a fresh run (e.g. they fixed the underlying cause), not an
    # automatic retry -- see DECISIONS.md.
    conn.execute(
        "UPDATE jobs SET state = ?, attempts = 0, next_retry_at = NULL, "
        "updated_at = ?, last_error = NULL WHERE id = ?",
        (JobState.PENDING, now_ts(), args.job_id),
    )
    print(f"requeued {args.job_id}")
    return 0


def cmd_config_set(args: argparse.Namespace) -> int:
    conn = db.connect()
    try:
        db.set_config(conn, args.key, args.value)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"{args.key} = {args.value}")
    return 0


def cmd_config_get(args: argparse.Namespace) -> int:
    conn = db.connect()
    try:
        print(db.get_config(conn, args.key))
    except KeyError:
        print(f"error: unknown config key: {args.key!r}", file=sys.stderr)
        return 1
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    webui.serve(args.port, args.host)
    return 0


def cmd_worker_start(args: argparse.Namespace) -> int:
    procs = [
        multiprocessing.Process(target=worker.run_forever, args=(i,))
        for i in range(args.count)
    ]
    for p in procs:
        p.start()
    print(f"started {len(procs)} worker(s): pids {[p.pid for p in procs]}")

    # Ctrl+C on this foreground process also reaches the children directly
    # (same POSIX process group), so they handle their own graceful
    # shutdown. This loop just keeps waiting for them to actually exit
    # instead of tearing down on the first KeyboardInterrupt.
    for p in procs:
        while p.is_alive():
            try:
                p.join(timeout=1)
            except KeyboardInterrupt:
                pass
    return 0


def cmd_worker_stop(args: argparse.Namespace) -> int:
    conn = db.connect()
    pids = registry.list_pids(conn)
    if not pids:
        print("no workers registered")
        return 0

    signaled = []
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            signaled.append(pid)
        except (ProcessLookupError, OSError):
            registry.unregister(conn, pid)  # stale entry, worker already gone

    print(f"sent stop signal to {len(signaled)} worker(s): {signaled}")

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        if not registry.list_pids(conn):
            print("all workers stopped")
            return 0
        time.sleep(0.5)

    remaining = registry.list_pids(conn)
    print(f"warning: {len(remaining)} worker(s) did not stop within {args.timeout}s: {remaining}", file=sys.stderr)
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queuectl", description="A CLI background job queue.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_enqueue = sub.add_parser("enqueue", help="add a new job")
    p_enqueue.add_argument("job_json", help='job as JSON, e.g. \'{"id":"job1","command":"sleep 2"}\'')
    p_enqueue.set_defaults(func=cmd_enqueue)

    p_list = sub.add_parser("list", help="list jobs")
    p_list.add_argument("--state", choices=JobState.ALL, help="filter by job state")
    p_list.add_argument("--json", action="store_true", help="print a JSON array to stdout")
    p_list.set_defaults(func=cmd_list)

    p_status = sub.add_parser("status", help="summary of job states and active workers")
    p_status.set_defaults(func=cmd_status)

    p_worker = sub.add_parser("worker", help="manage worker processes")
    worker_sub = p_worker.add_subparsers(dest="worker_command", required=True)

    p_worker_start = worker_sub.add_parser("start", help="start worker(s) in the foreground")
    p_worker_start.add_argument("--count", type=int, default=1, help="number of worker processes")
    p_worker_start.set_defaults(func=cmd_worker_start)

    p_worker_stop = worker_sub.add_parser("stop", help="gracefully stop all running workers")
    p_worker_stop.add_argument(
        "--timeout", type=float, default=30.0, help="seconds to wait for workers to exit"
    )
    p_worker_stop.set_defaults(func=cmd_worker_stop)

    p_dlq = sub.add_parser("dlq", help="inspect and retry dead-lettered jobs")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_command", required=True)

    p_dlq_list = dlq_sub.add_parser("list", help="list jobs in the DLQ")
    p_dlq_list.add_argument("--json", action="store_true", help="print a JSON array to stdout")
    p_dlq_list.set_defaults(func=cmd_dlq_list)

    p_dlq_retry = dlq_sub.add_parser("retry", help="re-enqueue a dead job")
    p_dlq_retry.add_argument("job_id")
    p_dlq_retry.set_defaults(func=cmd_dlq_retry)

    p_config = sub.add_parser("config", help="manage configuration")
    config_sub = p_config.add_subparsers(dest="config_command", required=True)

    p_config_set = config_sub.add_parser("set", help="set a config value")
    p_config_set.add_argument("key", choices=list(db.DEFAULT_CONFIG))
    p_config_set.add_argument("value")
    p_config_set.set_defaults(func=cmd_config_set)

    p_config_get = config_sub.add_parser("get", help="get a config value")
    p_config_get.add_argument("key", choices=list(db.DEFAULT_CONFIG))
    p_config_get.set_defaults(func=cmd_config_get)

    p_dashboard = sub.add_parser("dashboard", help="start a read-only web dashboard (bonus)")
    p_dashboard.add_argument("--port", type=int, default=8080)
    p_dashboard.add_argument("--host", default="127.0.0.1")
    p_dashboard.set_defaults(func=cmd_dashboard)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
