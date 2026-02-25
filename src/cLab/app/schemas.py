from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

MarketLiteral = Literal["spot", "futures"]
LayoutLiteral = Literal["mirror", "hive"]
VolumeTypeLiteral = Literal["base", "quote"]


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
