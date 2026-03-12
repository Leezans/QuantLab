from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from time import perf_counter

from quantlab.core.events import DomainEvent

NextCallable = Callable[[DomainEvent], None]


class BusMiddleware(ABC):
    @abstractmethod
    def __call__(self, event: DomainEvent, next_call: NextCallable) -> None:
        raise NotImplementedError


class LoggingMiddleware(BusMiddleware):
    def __call__(self, event: DomainEvent, next_call: NextCallable) -> None:
        print(
            f"[EventBus] event_type={event.event_type} "
            f"event_id={event.event_id} "
            f"source={event.source} "
            f"occurred_at={event.occurred_at.isoformat()}"
        )
        next_call(event)


class TimingMiddleware(BusMiddleware):
    def __call__(self, event: DomainEvent, next_call: NextCallable) -> None:
        start = perf_counter()
        try:
            next_call(event)
        finally:
            elapsed_ms = (perf_counter() - start) * 1000.0
            print(
                f"[EventBusTiming] event_type={event.event_type} "
                f"elapsed_ms={elapsed_ms:.3f}"
            )


class ExceptionMiddleware(BusMiddleware):
    def __call__(self, event: DomainEvent, next_call: NextCallable) -> None:
        try:
            next_call(event)
        except Exception as exc:
            print(
                f"[EventBusError] event_type={event.event_type} "
                f"event_id={event.event_id} "
                f"error={exc!r}"
            )
            raise
