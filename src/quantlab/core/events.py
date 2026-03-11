from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class MarketDataArrived:
    symbol: str
    timestamp: datetime
    last_price: float
    volume: float
    source: str  # e.g. "live.binance", "backtest.replay"


@dataclass(frozen=True, slots=True)
class FeatureCalculated:
    symbol: str
    timestamp: datetime
    feature_name: str
    feature_value: float
    source: str


@dataclass(frozen=True, slots=True)
class SignalGenerated:
    symbol: str
    timestamp: datetime
    side: str          # "BUY" | "SELL" | "FLAT"
    strength: float
    reason: str
    source: str


@dataclass(frozen=True, slots=True)
class RiskCheckPassed:
    symbol: str
    timestamp: datetime
    side: str
    strength: float
    source: str


@dataclass(frozen=True, slots=True)
class RiskCheckFailed:
    symbol: str
    timestamp: datetime
    side: str
    strength: float
    reason: str
    source: str


@dataclass(frozen=True, slots=True)
class OrderIntentCreated:
    symbol: str
    timestamp: datetime
    side: str
    quantity: float
    order_type: str    # "MARKET" | "LIMIT"
    source: str


@dataclass(frozen=True, slots=True)
class OrderSubmitted:
    order_id: str
    symbol: str
    timestamp: datetime
    side: str
    quantity: float
    order_type: str
    source: str


@dataclass(frozen=True, slots=True)
class OrderFilled:
    order_id: str
    symbol: str
    timestamp: datetime
    side: str
    quantity: float
    fill_price: float
    fees: float
    source: str


@dataclass(frozen=True, slots=True)
class PositionUpdated:
    symbol: str
    timestamp: datetime
    position_qty: float
    average_price: float
    source: str


@dataclass(frozen=True, slots=True)
class PortfolioValuated:
    timestamp: datetime
    nav: float
    cash: float
    marks: dict[str, float] = field(default_factory=dict)
    source: str = "portfolio"
