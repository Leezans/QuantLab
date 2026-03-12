from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


class JobStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobExecutionMode(str, Enum):
    THREAD = "thread"
    PROCESS = "process"


@dataclass(frozen=True, slots=True)
class JobSpec:
    job_type: str
    payload: dict[str, Any]
    dedupe_key: str | None = None
    execution_mode: JobExecutionMode = JobExecutionMode.THREAD
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobRecord:
    job_id: str
    job_type: str
    payload: dict[str, Any]
    status: JobStatus
    dedupe_key: str | None
    execution_mode: JobExecutionMode
    metadata: dict[str, Any]
    progress: float = 0.0
    message: str = ""
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @classmethod
    def create(cls, spec: JobSpec) -> "JobRecord":
        return cls(
            job_id=str(uuid4()),
            job_type=spec.job_type,
            payload=dict(spec.payload),
            status=JobStatus.PENDING,
            dedupe_key=spec.dedupe_key,
            execution_mode=spec.execution_mode,
            metadata=dict(spec.metadata),
        )

    @property
    def is_active(self) -> bool:
        return self.status in {JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING}

    @property
    def is_terminal(self) -> bool:
        return self.status in {JobStatus.SUCCEEDED, JobStatus.FAILED}
