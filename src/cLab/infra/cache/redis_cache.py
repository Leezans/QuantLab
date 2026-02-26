from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RedisCacheStub:
    """Optional cache stub. Replace with real Redis integration when enabled."""

    enabled: bool = False

    def get(self, key: str) -> str | None:
        _ = key
        return None

    def set(self, key: str, value: str) -> None:
        _ = (key, value)
