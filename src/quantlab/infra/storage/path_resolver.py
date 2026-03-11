from __future__ import annotations

from pathlib import Path


class PathResolver:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    def resolve(self, key: str) -> Path:
        return self._root / key