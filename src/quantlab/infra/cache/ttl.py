from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Generic, TypeVar

from .base import Cache

T = TypeVar("T")


@dataclass(slots=True)
class CacheEntry(Generic[T]):
    value: T
    expires_at: float


class TTLMemoryCache(Cache[T], Generic[T]):
    def __init__(self, ttl_seconds: float) -> None:
        self._ttl_seconds = ttl_seconds
        self._store: dict[str, CacheEntry[T]] = {}

    def get(self, key: str) -> T | None:
        entry = self._store.get(key)
        if entry is None:
            return None

        if time.time() > entry.expires_at:
            self._store.pop(key, None)
            return None

        return entry.value

    def set(self, key: str, value: T) -> None:
        self._store[key] = CacheEntry(
            value=value,
            expires_at=time.time() + self._ttl_seconds,
        )

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()