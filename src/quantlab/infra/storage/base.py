from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class BinaryStore(ABC):
    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> Path:
        raise NotImplementedError

    @abstractmethod
    def read_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        raise NotImplementedError