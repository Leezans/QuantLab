from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class Cache(ABC, Generic[T]):
    @abstractmethod
    def get(self, key: str) -> T | None:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: T) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError