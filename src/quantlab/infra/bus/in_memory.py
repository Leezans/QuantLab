from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from typing import DefaultDict

from .base import EventBus, EventHandler
from .middleware import BusMiddleware
from .types import EventEnvelope


class InMemoryEventBus(EventBus):
    def __init__(self, middlewares: list[BusMiddleware] | None = None) -> None:
        self._handlers: DefaultDict[str, list[EventHandler]] = defaultdict(list)
        self._middlewares = middlewares or []

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        self._handlers[event_type].append(handler)

    def publish(self, envelope: EventEnvelope) -> None:
        def dispatch(evt: EventEnvelope) -> None:
            for handler in self._handlers.get(evt.event_type, []):
                handler(evt)

        pipeline = dispatch
        for middleware in reversed(self._middlewares):
            next_call = pipeline

            def wrapper(evt: EventEnvelope, mw: BusMiddleware = middleware, nxt: Callable[[EventEnvelope], None] = next_call) -> None:
                mw(evt, nxt)

            pipeline = wrapper

        pipeline(envelope)