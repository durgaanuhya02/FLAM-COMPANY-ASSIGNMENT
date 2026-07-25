"""Argument parsing and command dispatch for the queuectl CLI."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import uuid
from typing import Sequence

from . import db
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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
