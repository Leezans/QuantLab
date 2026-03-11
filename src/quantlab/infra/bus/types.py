from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True, frozen=True)
class EventEnvelope:
    event_id: str
    event_type: str
    payload: Any
    occurred_at: datetime
    source: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def wrap(
        cls,
        payload: Any,
        *,
        source: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "EventEnvelope":
        return cls(
            event_id=str(uuid4()),
            event_type=type(payload).__name__,
            payload=payload,
            occurred_at=utc_now(),
            source=source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            metadata=metadata or {},
        )