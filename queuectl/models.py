"""Job data model and lifecycle states."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


class JobState:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"

    ALL = (PENDING, PROCESSING, COMPLETED, FAILED, DEAD)


def now_ts() -> float:
    """Current time as epoch seconds. All timing (backoff, leases) is done
    in epoch seconds internally; ISO 8601 strings are only for display."""
    return datetime.now(timezone.utc).timestamp()


def to_iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Job:
    id: str
    command: str
    state: str = JobState.PENDING
    attempts: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=now_ts)
    updated_at: float = field(default_factory=now_ts)
    next_retry_at: Optional[float] = None
    worker_pid: Optional[int] = None
    last_error: Optional[str] = None

    @classmethod
    def from_row(cls, row) -> "Job":
        return cls(
            id=row["id"],
            command=row["command"],
            state=row["state"],
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            next_retry_at=row["next_retry_at"],
            worker_pid=row["worker_pid"],
            last_error=row["last_error"],
        )

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "command": self.command,
            "state": self.state,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "created_at": to_iso(self.created_at),
            "updated_at": to_iso(self.updated_at),
        }
        if self.next_retry_at is not None:
            d["next_retry_at"] = to_iso(self.next_retry_at)
        if self.worker_pid is not None:
            d["worker_pid"] = self.worker_pid
        if self.last_error is not None:
            d["last_error"] = self.last_error
        return d
