from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class DomainEvent:
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: str = "domain.event"
    occurred_at: datetime = field(default_factory=utc_now)

@dataclass(frozen=True, slots=True)
class JobQueued(DomainEvent):
    job_id: str = ""
    event_type: str = "job.queued"


@dataclass(frozen=True, slots=True)
class JobStarted(DomainEvent):
    job_id: str = ""
    event_type: str = "job.started"


@dataclass(frozen=True, slots=True)
class JobProgressed(DomainEvent):
    job_id: str = ""
    progress: float = 0.0
    message: str = ""
    event_type: str = "job.progressed"


@dataclass(frozen=True, slots=True)
class JobSucceeded(DomainEvent):
    job_id: str = ""
    result: dict[str, Any] | None = None
    event_type: str = "job.succeeded"


@dataclass(frozen=True, slots=True)
class JobFailed(DomainEvent):
    job_id: str = ""
    error: str = ""
    event_type: str = "job.failed"



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
