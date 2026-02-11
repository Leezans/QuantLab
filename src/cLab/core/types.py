from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(frozen=True)
class Kline:
    """OHLCV bar."""

    symbol: str
    interval: str
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class PriceTick:
    symbol: str
    ts: datetime
    price: float


Side = Literal["buy", "sell"]


@dataclass(frozen=True)
class Order:
    symbol: str
    side: Side
    qty: float
    price: float | None = None


@dataclass(frozen=True)
class Fill:
    order_id: str
    symbol: str
    side: Side
    qty: float
    price: float
    ts: datetime
