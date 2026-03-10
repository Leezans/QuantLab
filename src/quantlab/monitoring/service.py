from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class HealthEvent:
    timestamp: datetime
    service: str
    status: str
    detail: str = ""


class MonitoringService:
    def __init__(self) -> None:
        self._events: list[HealthEvent] = []

    def record(self, service: str, status: str, detail: str = "") -> HealthEvent:
        event = HealthEvent(timestamp=datetime.now(timezone.utc), service=service, status=status, detail=detail)
        self._events.append(event)
        return event

    def latest(self) -> HealthEvent | None:
        return self._events[-1] if self._events else None

    def summary(self) -> dict[str, str]:
        latest_by_service: dict[str, str] = {}
        for event in self._events:
            latest_by_service[event.service] = event.status
        return latest_by_service

