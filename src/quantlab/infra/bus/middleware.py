from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from .types import EventEnvelope

NextCallable = Callable[[EventEnvelope], None]


class BusMiddleware(ABC):
    @abstractmethod
    def __call__(self, envelope: EventEnvelope, next_call: NextCallable) -> None:
        raise NotImplementedError


class LoggingMiddleware(BusMiddleware):
    def __call__(self, envelope: EventEnvelope, next_call: NextCallable) -> None:
        print(
            f"[EventBus] event_type={envelope.event_type} "
            f"event_id={envelope.event_id} occurred_at={envelope.occurred_at.isoformat()}"
        )
        next_call(envelope)