from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True, frozen=True)
class LoggedEvent:
    sequence: int
    event_id: str
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime
    source: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        sequence: int,
        event_type: str,
        payload: dict[str, Any],
        *,
        source: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "LoggedEvent":
        return cls(
            sequence=sequence,
            event_id=str(uuid4()),
            event_type=event_type,
            payload=payload,
            occurred_at=utc_now(),
            source=source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            metadata=metadata or {},
        )


class EventLog(ABC):
    @abstractmethod
    def append(self, event: LoggedEvent) -> None:
        raise NotImplementedError

    @abstractmethod
    def read_all(self) -> Iterable[LoggedEvent]:
        raise NotImplementedError

    @abstractmethod
    def next_sequence(self) -> int:
        raise NotImplementedError
