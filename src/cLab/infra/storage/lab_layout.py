from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LabLayout:
    """A simple hive-style file layout for lab-generated datasets.

    Layout:
        <root>/<dataset>/<symbol>/<YYYY-MM-DD>/<filename>

    Examples:
        <root>/aggtrades_raw/BTCUSDT/2026-02-11/part-0000.jsonl
        <root>/trade_features_1m/BTCUSDT/2026-02-11/part-0000.parquet
        <root>/bars_1m/BTCUSDT/2026-02-11/part-0000.parquet
        <root>/bars_1m/BTCUSDT/2026-02-11/manifest.json
    """

    root: str

    def dataset_dir(self, dataset: str) -> Path:
        return Path(self.root) / dataset

    def symbol_dir(self, dataset: str, symbol: str) -> Path:
        return self.dataset_dir(dataset) / symbol

    def date_dir(self, dataset: str, symbol: str, date: str) -> Path:
        return self.symbol_dir(dataset, symbol) / date

    def file_path(self, dataset: str, symbol: str, date: str, filename: str) -> Path:
        return self.date_dir(dataset, symbol, date) / filename

    def manifest_path(self, dataset: str, symbol: str, date: str) -> Path:
        return self.file_path(dataset, symbol, date, "manifest.json")

    # -----------------
    # Discovery helpers
    # -----------------
    def list_datasets(self) -> list[str]:
        root = Path(self.root)
        if not root.exists():
            return []
        return sorted([p.name for p in root.iterdir() if p.is_dir()])

    def list_symbols(self, dataset: str) -> list[str]:
        p = self.dataset_dir(dataset)
        if not p.exists():
            return []
        return sorted([x.name for x in p.iterdir() if x.is_dir()])

    def list_dates(self, dataset: str, symbol: str) -> list[str]:
        p = self.symbol_dir(dataset, symbol)
        if not p.exists():
            return []
        return sorted([x.name for x in p.iterdir() if x.is_dir()])
