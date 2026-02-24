from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ui.services.types.common import (
    EnsureFactorsRequest,
    EnsureFactorsResult,
    EnsureKlinesRequest,
    EnsureKlinesResult,
    TaskRef,
    TaskStatus,
)
from ui.services.types.cryptos import TradesRangeRequest, TradesRangeResult


_SLAB_UNAVAILABLE = "sLab backend is not implemented yet. UI interface is reserved."


@dataclass
class SLabPlaceholderService:
    _lab_key: str = "stocks"

    def lab_key(self) -> str:
        return self._lab_key

    def display_name(self) -> str:
        return "sLab (UI Only)"

    def list_symbols(self) -> list[str]:
        return ["AAPL", "MSFT", "600519.SH", "000001.SZ"]

    def supports_trades_download(self) -> bool:
        return False

    def ensure_klines(self, req: EnsureKlinesRequest) -> EnsureKlinesResult:
        return EnsureKlinesResult(
            symbol=req.symbol.strip().upper(),
            interval=req.interval,
            source="unavailable",
            dataframe=pd.DataFrame(),
            total_days=0,
            cached_days=0,
            fetched_days=0,
            failed_days=1,
            parquet_paths=[],
            errors=[_SLAB_UNAVAILABLE],
        )

    def ensure_factors(self, req: EnsureFactorsRequest) -> EnsureFactorsResult:
        return EnsureFactorsResult(
            symbol=req.symbol.strip().upper(),
            factor_set=req.factor_set,
            source="unavailable",
            dataframe=pd.DataFrame(),
            cache_path="",
            input_source="unavailable",
            errors=[_SLAB_UNAVAILABLE],
        )

    def run_trades_range(self, req: TradesRangeRequest) -> TradesRangeResult:
        return TradesRangeResult(
            symbol=req.symbol.strip().upper(),
            source="unavailable",
            total_days=0,
            ok=0,
            skipped=0,
            failed=1,
            parquet_paths=[],
            errors=[_SLAB_UNAVAILABLE],
        )

    def start_task(self, name: str, payload: dict) -> TaskRef:
        return TaskRef(task_id=f"pending-{name}", status="unavailable", detail={"reason": _SLAB_UNAVAILABLE})

    def get_task(self, task_id: str) -> TaskStatus:
        return TaskStatus(task_id=task_id, status="unavailable", detail={"reason": _SLAB_UNAVAILABLE})


# Backward-compatible alias.
StocksLabService = SLabPlaceholderService
