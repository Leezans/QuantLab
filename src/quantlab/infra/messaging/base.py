from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from .envelope import MessageEnvelope

MessageHandler = Callable[[MessageEnvelope], None]


class MessageChannel(ABC):
    @abstractmethod
    def subscribe(self, topic: str, handler: MessageHandler) -> None:
        raise NotImplementedError

    @abstractmethod
    def publish(self, message: MessageEnvelope) -> None:
        raise NotImplementedError