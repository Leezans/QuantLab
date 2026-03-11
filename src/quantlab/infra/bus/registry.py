from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict

from quantlab.infra.bus.interfaces import EventHandler


class SubscriptionRegistry:
    """
    event_type -> ordered handlers
    """

    def __init__(self) -> None:
        self._handlers: DefaultDict[str, list[EventHandler]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        self._handlers[event_type].append(handler)

    def get_handlers(self, event_type: str) -> list[EventHandler]:
        return list(self._handlers.get(event_type, []))

    def has_subscribers(self, event_type: str) -> bool:
        return event_type in self._handlers and len(self._handlers[event_type]) > 0