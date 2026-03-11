from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict

from .base import MessageChannel, MessageHandler
from .envelope import MessageEnvelope


class InMemoryMessageChannel(MessageChannel):
    def __init__(self) -> None:
        self._subscribers: DefaultDict[str, list[MessageHandler]] = defaultdict(list)

    def subscribe(self, topic: str, handler: MessageHandler) -> None:
        self._subscribers[topic].append(handler)

    def publish(self, message: MessageEnvelope) -> None:
        for handler in self._subscribers.get(message.topic, []):
            handler(message)