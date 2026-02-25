from __future__ import annotations

from typing import Protocol

from ui.services.types.common import (
    EnsureFactorsRequest,
    EnsureFactorsResult,
    EnsureKlinesRequest,
    EnsureKlinesResult,
    TaskRef,
    TaskStatus,
)
from ui.services.types.cryptos import (
    KlinesRangeRequestDTO,
    KlinesRangeResultDTO,
    TradesRangeRequest,
    TradesRangeRequestDTO,
    TradesRangeResult,
    TradesRangeResultDTO,
)


class LabService(Protocol):
    def lab_key(self) -> str: ...
    def display_name(self) -> str: ...
    def list_symbols(self) -> list[str]: ...
    def supports_trades_download(self) -> bool: ...
    def ensure_klines(self, req: EnsureKlinesRequest) -> EnsureKlinesResult: ...
    def ensure_factors(self, req: EnsureFactorsRequest) -> EnsureFactorsResult: ...
    def run_trades_range(self, req: TradesRangeRequest) -> TradesRangeResult: ...
    def start_task(self, name: str, payload: dict) -> TaskRef: ...
    def get_task(self, task_id: str) -> TaskStatus: ...


class MarketDataService(Protocol):
    """Adapter protocol consumed by market-data orchestrators."""

    def get_or_create_klines_range(self, req: KlinesRangeRequestDTO) -> KlinesRangeResultDTO: ...
    def get_or_create_trades_range(self, req: TradesRangeRequestDTO) -> TradesRangeResultDTO: ...
