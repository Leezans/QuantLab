from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field

from cLab.app.dto.types import LayoutLiteral, MarketLiteral


class BacktestRunRequestDTO(BaseModel):
    symbol: str = Field(..., min_length=1)
    start: date
    end: date
    interval: str = "1h"
    market: MarketLiteral = "spot"
    style: LayoutLiteral = "mirror"
    fast_window: int = Field(20, ge=1)
    slow_window: int = Field(60, ge=2)
    fee_bps: float = Field(2.0, ge=0.0)
    slippage_bps: float = Field(1.0, ge=0.0)
    initial_cash: float = Field(10_000.0, gt=0.0)
    seed: int | None = None


class FeatureBuildRequestDTO(BaseModel):
    symbol: str = Field(..., min_length=1)
    start: date
    end: date
    factor_set: list[str] = Field(..., min_length=1)
    interval: str = "1h"
    market: MarketLiteral = "spot"
    style: LayoutLiteral = "mirror"
    factor_params: dict[str, dict[str, Any]] | None = None


__all__ = ["BacktestRunRequestDTO", "FeatureBuildRequestDTO"]

