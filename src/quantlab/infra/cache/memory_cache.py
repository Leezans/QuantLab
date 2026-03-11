from __future__ import annotations

from typing import Generic, TypeVar

from .base import Cache

T = TypeVar("T")


class MemoryCache(Cache[T], Generic[T]):
    def __init__(self) -> None:
        self._store: dict[str, T] = {}

    def get(self, key: str) -> T | None:
        return self._store.get(key)

    def set(self, key: str, value: T) -> None:
        self._store[key] = value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()