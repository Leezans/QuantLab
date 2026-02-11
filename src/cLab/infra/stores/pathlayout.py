from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PathLayout:
    """A simple folder layout for file-based datasets.

    Layout:
        <root>/<dataset>/<symbol>/<YYYY-MM-DD>/<filename>

    This is intentionally minimal and cross-platform.
    """

    root: str

    def dataset_dir(self, dataset: str) -> Path:
        return Path(self.root) / dataset

    def symbol_dir(self, dataset: str, symbol: str) -> Path:
        return self.dataset_dir(dataset) / symbol

    def date_dir(self, dataset: str, symbol: str, date: str) -> Path:
        # date: YYYY-MM-DD
        return self.symbol_dir(dataset, symbol) / date

    def file_path(self, dataset: str, symbol: str, date: str, filename: str) -> Path:
        return self.date_dir(dataset, symbol, date) / filename
