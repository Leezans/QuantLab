from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict

from quantlab.core.events import DomainEvent
from quantlab.core.interfaces import EventHandler


class SubscriptionRegistry:
    """
    event_type -> ordered handlers
    """

    def __init__(self) -> None:
        self._handlers: DefaultDict[str, list[EventHandler]] = defaultdict(list)

    def subscribe(self, event_type: str | type[DomainEvent], handler: EventHandler) -> None:
        key = event_type if isinstance(event_type, str) else event_type.event_name()
        self._handlers[key].append(handler)

    def get_handlers(self, event_type: str) -> list[EventHandler]:
        return list(self._handlers.get(event_type, []))
