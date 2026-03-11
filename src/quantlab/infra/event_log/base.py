from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from .models import LoggedEvent


class EventLog(ABC):
    @abstractmethod
    def append(self, event: LoggedEvent) -> None:
        raise NotImplementedError

    @abstractmethod
    def read_all(self) -> Iterable[LoggedEvent]:
        raise NotImplementedError

    @abstractmethod
    def next_sequence(self) -> int:
        raise NotImplementedError