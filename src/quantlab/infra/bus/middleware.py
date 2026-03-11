from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from time import perf_counter

from quantlab.infra.bus.types import EventEnvelope

NextCallable = Callable[[EventEnvelope], None]


class BusMiddleware(ABC):
    @abstractmethod
    def __call__(self, envelope: EventEnvelope, next_call: NextCallable) -> None:
        raise NotImplementedError


class LoggingMiddleware(BusMiddleware):
    def __call__(self, envelope: EventEnvelope, next_call: NextCallable) -> None:
        print(
            f"[EventBus] event_type={envelope.event_type} "
            f"event_id={envelope.event_id} "
            f"source={envelope.source} "
            f"occurred_at={envelope.occurred_at.isoformat()}"
        )
        next_call(envelope)


class TimingMiddleware(BusMiddleware):
    def __call__(self, envelope: EventEnvelope, next_call: NextCallable) -> None:
        start = perf_counter()
        try:
            next_call(envelope)
        finally:
            elapsed_ms = (perf_counter() - start) * 1000.0
            print(
                f"[EventBusTiming] event_type={envelope.event_type} "
                f"elapsed_ms={elapsed_ms:.3f}"
            )


class ExceptionMiddleware(BusMiddleware):
    def __call__(self, envelope: EventEnvelope, next_call: NextCallable) -> None:
        try:
            next_call(envelope)
        except Exception as exc:
            print(
                f"[EventBusError] event_type={envelope.event_type} "
                f"event_id={envelope.event_id} "
                f"error={exc!r}"
            )
            raise