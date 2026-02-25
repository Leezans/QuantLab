from __future__ import annotations

from dataclasses import dataclass
import os

from ui.services.types.common import (
    EnsureFactorsRequest,
    EnsureFactorsResult,
    EnsureKlinesRequest,
    EnsureKlinesResult,
    TaskRef,
    TaskStatus,
)
from ui.services.types.cryptos import (
    KlinesRequestDTO,
    KlinesResultDTO,
    TradesRequestDTO,
    TradesResultDTO,
)


@dataclass
class BaseHTTPService:
    _lab_key: str
    _display_name: str
    _supports_trades_download: bool
    _symbols: list[str]
    _base_url: str | None = None

    def __post_init__(self):
        if self._base_url is None:
            self._base_url = os.getenv("QUANTLAB_API_BASE_URL")

    def lab_key(self) -> str:
        return self._lab_key

    def display_name(self) -> str:
        return self._display_name

    def list_symbols(self) -> list[str]:
        return list(self._symbols)

    def supports_trades_download(self) -> bool:
        return self._supports_trades_download

    def ensure_klines(self, req: EnsureKlinesRequest) -> EnsureKlinesResult:
        raise NotImplementedError("HTTP service is scaffolded but endpoint mapping is not implemented yet.")

    def ensure_factors(self, req: EnsureFactorsRequest) -> EnsureFactorsResult:
        raise NotImplementedError("HTTP service is scaffolded but endpoint mapping is not implemented yet.")

    def run_trades_range(self, req: TradesRequestDTO) -> TradesResultDTO:
        raise NotImplementedError("HTTP service is scaffolded but endpoint mapping is not implemented yet.")

    def start_task(self, name: str, payload: dict) -> TaskRef:
        raise NotImplementedError("HTTP service is scaffolded but endpoint mapping is not implemented yet.")

    def get_task(self, task_id: str) -> TaskStatus:
        raise NotImplementedError("HTTP service is scaffolded but endpoint mapping is not implemented yet.")

    def get_or_create_klines(self, req: KlinesRequestDTO) -> KlinesResultDTO:
        raise NotImplementedError("HTTP market-data adapter is scaffolded but not implemented yet.")

    def get_or_create_trades(self, req: TradesRequestDTO) -> TradesResultDTO:
        raise NotImplementedError("HTTP market-data adapter is scaffolded but not implemented yet.")
