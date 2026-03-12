from __future__ import annotations

from pathlib import Path

from quantlab.core.storage import BinaryStore
from .path_resolver import PathResolver


class LocalFileStore(BinaryStore):
    def __init__(self, root: str | Path) -> None:
        self._resolver = PathResolver(root)
        self._resolver.root.mkdir(parents=True, exist_ok=True)

    def write_bytes(self, key: str, data: bytes) -> Path:
        path = self._resolver.resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def read_bytes(self, key: str) -> bytes:
        path = self._resolver.resolve(key)
        return path.read_bytes()

    def exists(self, key: str) -> bool:
        return self._resolver.resolve(key).exists()
