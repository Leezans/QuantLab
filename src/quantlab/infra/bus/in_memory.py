from __future__ import annotations

from collections.abc import Callable

from quantlab.infra.bus.interfaces import EventBus, EventHandler
from quantlab.infra.bus.middleware import BusMiddleware
from quantlab.infra.bus.registry import SubscriptionRegistry
from quantlab.infra.bus.types import EventEnvelope


class InMemoryEventBus(EventBus):
    def __init__(
        self,
        registry: SubscriptionRegistry | None = None,
        middlewares: list[BusMiddleware] | None = None,
    ) -> None:
        self._registry = registry or SubscriptionRegistry()
        self._middlewares = middlewares or []

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        self._registry.subscribe(event_type, handler)

    def publish(self, envelope: EventEnvelope) -> None:
        handlers = self._registry.get_handlers(envelope.event_type)

        def dispatch(evt: EventEnvelope) -> None:
            for handler in handlers:
                handler(evt)

        pipeline: Callable[[EventEnvelope], None] = dispatch

        for middleware in reversed(self._middlewares):
            next_call = pipeline

            def wrapper(
                evt: EventEnvelope,
                mw: BusMiddleware = middleware,
                nxt: Callable[[EventEnvelope], None] = next_call,
            ) -> None:
                mw(evt, nxt)

            pipeline = wrapper

        pipeline(envelope)

    def publish_event(
        self,
        event: object,
        *,
        source: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        envelope = EventEnvelope.wrap(
            event,
            source=source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            metadata=metadata,
        )
        self.publish(envelope)