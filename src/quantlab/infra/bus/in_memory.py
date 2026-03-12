from __future__ import annotations

from collections.abc import Callable

from quantlab.core.events import DomainEvent
from quantlab.core.interfaces import EventBus, EventHandler
from quantlab.infra.bus.middleware import BusMiddleware
from quantlab.infra.bus.registry import SubscriptionRegistry


class InMemoryEventBus(EventBus):
    def __init__(
        self,
        registry: SubscriptionRegistry | None = None,
        middlewares: list[BusMiddleware] | None = None,
    ) -> None:
        self._registry = registry or SubscriptionRegistry()
        self._middlewares = middlewares or []

    def subscribe(self, event_type: str | type[DomainEvent], handler: EventHandler) -> None:
        self._registry.subscribe(event_type, handler)

    def publish(self, event: DomainEvent) -> None:
        handlers = self._registry.get_handlers(event.event_type)
        handlers.extend(self._registry.get_handlers("*"))

        def dispatch(evt: DomainEvent) -> None:
            for handler in handlers:
                handler(evt)

        pipeline: Callable[[DomainEvent], None] = dispatch

        for middleware in reversed(self._middlewares):
            next_call = pipeline

            def wrapper(
                evt: DomainEvent,
                mw: BusMiddleware = middleware,
                nxt: Callable[[DomainEvent], None] = next_call,
            ) -> None:
                mw(evt, nxt)

            pipeline = wrapper

        pipeline(event)
