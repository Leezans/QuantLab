from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True, frozen=True)
class MessageEnvelope:
    message_id: str
    topic: str
    payload: Any
    created_at: datetime
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        topic: str,
        payload: Any,
        headers: dict[str, str] | None = None,
    ) -> "MessageEnvelope":
        return cls(
            message_id=str(uuid4()),
            topic=topic,
            payload=payload,
            created_at=utc_now(),
            headers=headers or {},
        )