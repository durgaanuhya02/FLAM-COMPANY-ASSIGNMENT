"""Minimal read-only web dashboard (bonus feature).

Deliberately built on the stdlib http.server instead of a framework, to
keep the "no external dependencies" property that holds for the rest of
the project. It only ever runs SELECTs -- there is no code path here
that can mutate a job, by construction (the handler only implements
do_GET, never do_POST/do_PUT/etc).

Each request opens its own short-lived DB connection instead of sharing
one across threads: ThreadingHTTPServer serves each request on its own
thread, and sqlite3 connections aren't safe to share across threads
without extra care, so this sidesteps that entirely rather than reasoning
about it.
"""

from __future__ import annotations

import html
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import db
from .models import Job, JobState

PAGE_TEMPLATE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="3">
<title>queuectl dashboard</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1 {{ font-size: 1.15rem; font-weight: 600; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
  th, td {{ text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #ddd; font-size: 0.82rem; }}
  th {{ background: #f5f5f5; }}
  .summary {{ display: flex; gap: 1rem; flex-wrap: wrap; }}
  .summary div {{ background: #f5f5f5; padding: 0.5rem 1rem; border-radius: 6px; font-size: 0.85rem; }}
  .state-completed {{ color: #1a7f37; }}
  .state-dead {{ color: #b3261e; font-weight: 600; }}
  .state-failed {{ color: #b35900; }}
  .state-processing {{ color: #0969da; }}
  code {{ font-size: 0.8rem; }}
  .muted {{ color: #666; font-size: 0.8rem; }}
</style>
</head>
<body>
<h1>queuectl dashboard <span class="muted">(read-only, auto-refreshes every 3s)</span></h1>
<div class="summary">{summary_html}</div>
<p class="muted">active workers: {active_workers} &middot; <a href="/api/jobs">/api/jobs</a> for JSON</p>
<table>
<tr><th>id</th><th>state</th><th>attempts</th><th>command</th><th>updated_at</th><th>last_error</th></tr>
{rows_html}
</table>
</body>
</html>
"""


def _render_dashboard(conn) -> str:
    counts = dict.fromkeys(JobState.ALL, 0)
    for row in conn.execute("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state"):
        counts[row["state"]] = row["n"]
    active_workers = conn.execute("SELECT COUNT(*) AS n FROM workers").fetchone()["n"]

    summary_html = "".join(f"<div>{state}: <b>{counts[state]}</b></div>" for state in JobState.ALL)

    rows = conn.execute("SELECT * FROM jobs ORDER BY updated_at DESC LIMIT 200").fetchall()
    row_tpl = (
        "<tr class='state-{state}'><td><code>{id}</code></td><td>{state}</td>"
        "<td>{attempts}/{max_retries}</td><td><code>{command}</code></td>"
        "<td>{updated_at}</td><td>{last_error}</td></tr>"
    )
    rows_html = "".join(
        row_tpl.format(
            id=html.escape(r["id"]),
            state=html.escape(r["state"]),
            attempts=r["attempts"],
            max_retries=r["max_retries"],
            command=html.escape(r["command"]),
            updated_at=html.escape(Job.from_row(r).to_dict().get("updated_at") or ""),
            last_error=html.escape(r["last_error"] or ""),
        )
        for r in rows
    )
    if not rows_html:
        rows_html = "<tr><td colspan='6' class='muted'>no jobs yet</td></tr>"

    return PAGE_TEMPLATE.format(summary_html=summary_html, active_workers=active_workers, rows_html=rows_html)


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # keep this out of the worker/CLI output; not an error channel

    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        conn = db.connect()
        try:
            if parsed.path == "/":
                self._send(200, "text/html; charset=utf-8", _render_dashboard(conn).encode("utf-8"))
            elif parsed.path == "/api/jobs":
                state = parse_qs(parsed.query).get("state", [None])[0]
                if state:
                    rows = conn.execute(
                        "SELECT * FROM jobs WHERE state = ? ORDER BY updated_at DESC", (state,)
                    ).fetchall()
                else:
                    rows = conn.execute("SELECT * FROM jobs ORDER BY updated_at DESC").fetchall()
                body = json.dumps([Job.from_row(r).to_dict() for r in rows]).encode("utf-8")
                self._send(200, "application/json", body)
            else:
                self._send(404, "text/plain; charset=utf-8", b"not found")
        finally:
            conn.close()


def serve(port: int, host: str = "127.0.0.1") -> None:
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"queuectl dashboard: http://{host}:{port}/ (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
