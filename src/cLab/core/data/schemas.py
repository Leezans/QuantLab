# src/cLab/core/data/schemas.py
from __future__ import annotations
import pandas as pd


class KlinesRangeResult:
    def __init__(
        self,
        symbol: str,
        market: str,
        source: str,
        total_days: int,
        ok: int,
        skipped: int,
        failed: int,
        row_count: int,
        parquet_paths: list[str],
        errors: list[str],
        preview: pd.DataFrame,
    ) -> None:
        self.symbol = symbol
        self.market = market
        self.source = source
        self.total_days = total_days
        self.ok = ok
        self.skipped = skipped
        self.failed = failed
        self.row_count = row_count
        self.parquet_paths = parquet_paths
        self.errors = errors
        self.preview = preview

class TradesRangeResult:
    def __init__(
        self,
        symbol: str,
        market: str,
        source: str,
        total_days: int,
        ok: int,
        skipped: int,
        failed: int,
        row_count: int,
        parquet_paths: list[str],
        errors: list[str],
        preview: pd.DataFrame,
    ) -> None:
        self.symbol = symbol
        self.market = market
        self.source = source
        self.total_days = total_days
        self.ok = ok
        self.skipped = skipped
        self.failed = failed
        self.row_count = row_count
        self.parquet_paths = parquet_paths
        self.errors = errors
        self.preview = preview