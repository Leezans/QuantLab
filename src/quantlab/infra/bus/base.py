from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from .types import EventEnvelope

EventHandler = Callable[[EventEnvelope], None]


class EventBus(ABC):
    @abstractmethod
    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        raise NotImplementedError

    @abstractmethod
    def publish(self, envelope: EventEnvelope) -> None:
        raise NotImplementedError

    def publish_event(self, event: Any) -> None:
        self.publish(EventEnvelope.wrap(event))