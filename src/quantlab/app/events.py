from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

from quantlab.core.events import DomainEvent


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
