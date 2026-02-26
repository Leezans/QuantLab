from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd

from cLab.core.domain.types import RunRecord


@dataclass(frozen=True)
class EnsureRangeResult:
    symbol: str
    market: str
    source: str
    total_days: int
    ok: int
    skipped: int
    failed: int
    row_count: int
    parquet_paths: list[str]
    errors: list[str]
    preview: pd.DataFrame


class MarketDataStore(Protocol):
    def ensure_klines_range(
        self,
        *,
        symbol: str,
        start: str,
        end: str,
        interval: str,
        market: str,
        style: str,
        preview_rows: int,
        fetch_checksum: bool,
        verify_checksum: bool,
        compression: str,
        raise_on_error: bool,
    ) -> EnsureRangeResult:
        ...

    def ensure_trades_range(
        self,
        *,
        symbol: str,
        start: str,
        end: str,
        market: str,
        style: str,
        preview_rows: int,
        fetch_checksum: bool,
        verify_checksum: bool,
        compression: str,
        raise_on_error: bool,
    ) -> EnsureRangeResult:
        ...

    def compute_volume_profile_from_parquet(
        self,
        parquet_paths: list[str],
        *,
        bins: int,
        volume_type: str,
        normalize: bool,
        start_ts: int | None = None,
        end_ts: int | None = None,
        max_rows: int | None = None,
    ) -> tuple[list[float], list[float]]:
        ...

    def load_bars(
        self,
        *,
        symbol: str,
        start: date,
        end: date,
        interval: str,
        market: str,
        style: str,
    ) -> pd.DataFrame:
        ...


class FeatureStore(Protocol):
    def save_features(self, symbol: str, factor_set: Sequence[str], frame: pd.DataFrame) -> str:
        ...


class ExperimentStore(Protocol):
    def save_run(self, record: RunRecord, payload: dict[str, Any]) -> str:
        ...

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        ...

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        ...

