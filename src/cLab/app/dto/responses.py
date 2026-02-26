from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from cLab.app.dto.types import MarketLiteral, VolumeTypeLiteral


class PipelineStatsDTO(BaseModel):
    total_days: int
    ok: int
    skipped: int
    failed: int


class KlinePointDTO(BaseModel):
    time: int = Field(..., description="Unix timestamp in seconds")
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


class TradePointDTO(BaseModel):
    time: int = Field(..., description="Unix timestamp in seconds")
    price: float
    quantity: float | None = None
    quote_quantity: float | None = None


class VolumeProfileBinDTO(BaseModel):
    price: float
    volume: float


class KlinesResponseDTO(BaseModel):
    symbol: str
    market: MarketLiteral
    interval: str
    start: str
    end: str
    source: str
    stats: PipelineStatsDTO
    row_count: int
    parquet_paths: list[str]
    errors: list[str]
    preview: list[KlinePointDTO]


class TradesResponseDTO(BaseModel):
    symbol: str
    market: MarketLiteral
    start: str
    end: str
    source: str
    stats: PipelineStatsDTO
    row_count: int
    parquet_paths: list[str]
    errors: list[str]
    preview: list[TradePointDTO]


class VolumeProfileResponseDTO(BaseModel):
    symbol: str
    market: MarketLiteral
    start: str
    end: str
    bins: int
    volume_type: VolumeTypeLiteral
    normalized: bool
    profile: list[VolumeProfileBinDTO]
    trades_source: str
    trades_row_count: int
    errors: list[str]


class HealthResponseDTO(BaseModel):
    status: str


class BacktestSummaryDTO(BaseModel):
    total_return: float
    max_drawdown: float
    sharpe_ratio: float
    final_equity: float
    trade_count: int


class BacktestRunResponseDTO(BaseModel):
    run_id: str
    artifact_path: str
    summary: BacktestSummaryDTO


class BacktestRecordResponseDTO(BaseModel):
    run: dict[str, Any]


class FeatureBuildResponseDTO(BaseModel):
    artifact_path: str
    row_count: int


class ResearchRunsResponseDTO(BaseModel):
    runs: list[dict[str, Any]]


__all__ = [
    "BacktestRecordResponseDTO",
    "BacktestRunResponseDTO",
    "BacktestSummaryDTO",
    "FeatureBuildResponseDTO",
    "HealthResponseDTO",
    "KlinePointDTO",
    "KlinesResponseDTO",
    "PipelineStatsDTO",
    "ResearchRunsResponseDTO",
    "TradePointDTO",
    "TradesResponseDTO",
    "VolumeProfileBinDTO",
    "VolumeProfileResponseDTO",
]

