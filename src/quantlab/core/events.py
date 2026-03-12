from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True, kw_only=True)
class DomainEvent:
    event_id: str = field(default_factory=lambda: str(uuid4()))
    occurred_at: datetime = field(default_factory=utc_now)
    source: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    EVENT_TYPE: ClassVar[str] = "domain.event"

    @property
    def event_type(self) -> str:
        return self.EVENT_TYPE

    @classmethod
    def event_name(cls) -> str:
        return cls.EVENT_TYPE

@dataclass(frozen=True, slots=True)
class JobQueued(DomainEvent):
    job_id: str

    EVENT_TYPE: ClassVar[str] = "job.queued"


@dataclass(frozen=True, slots=True)
class JobStarted(DomainEvent):
    job_id: str

    EVENT_TYPE: ClassVar[str] = "job.started"


@dataclass(frozen=True, slots=True)
class JobProgressed(DomainEvent):
    job_id: str
    progress: float
    message: str = ""

    EVENT_TYPE: ClassVar[str] = "job.progressed"


@dataclass(frozen=True, slots=True)
class JobSucceeded(DomainEvent):
    job_id: str
    result: dict[str, Any] | None = None

    EVENT_TYPE: ClassVar[str] = "job.succeeded"


@dataclass(frozen=True, slots=True)
class JobFailed(DomainEvent):
    job_id: str
    error: str

    EVENT_TYPE: ClassVar[str] = "job.failed"






