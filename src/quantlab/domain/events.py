from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar

from quantlab.core.events import DomainEvent


@dataclass(frozen=True, slots=True)
class MarketDataArrived(DomainEvent):
    symbol: str
    timestamp: datetime
    last_price: float
    volume: float

    EVENT_TYPE: ClassVar[str] = "market_data.arrived"


@dataclass(frozen=True, slots=True)
class FeatureCalculated(DomainEvent):
    symbol: str
    timestamp: datetime
    feature_name: str
    feature_value: float

    EVENT_TYPE: ClassVar[str] = "feature.calculated"


@dataclass(frozen=True, slots=True)
class SignalGenerated(DomainEvent):
    symbol: str
    timestamp: datetime
    side: str
    strength: float
    reason: str

    EVENT_TYPE: ClassVar[str] = "signal.generated"


@dataclass(frozen=True, slots=True)
class RiskCheckPassed(DomainEvent):
    symbol: str
    timestamp: datetime
    side: str
    strength: float

    EVENT_TYPE: ClassVar[str] = "risk_check.passed"


@dataclass(frozen=True, slots=True)
class RiskCheckFailed(DomainEvent):
    symbol: str
    timestamp: datetime
    side: str
    strength: float
    reason: str

    EVENT_TYPE: ClassVar[str] = "risk_check.failed"


@dataclass(frozen=True, slots=True)
class OrderIntentCreated(DomainEvent):
    symbol: str
    timestamp: datetime
    side: str
    quantity: float
    order_type: str

    EVENT_TYPE: ClassVar[str] = "order_intent.created"


@dataclass(frozen=True, slots=True)
class OrderSubmitted(DomainEvent):
    order_id: str
    symbol: str
    timestamp: datetime
    side: str
    quantity: float
    order_type: str

    EVENT_TYPE: ClassVar[str] = "order.submitted"


@dataclass(frozen=True, slots=True)
class OrderFilled(DomainEvent):
    order_id: str
    symbol: str
    timestamp: datetime
    side: str
    quantity: float
    fill_price: float
    fees: float

    EVENT_TYPE: ClassVar[str] = "order.filled"


@dataclass(frozen=True, slots=True)
class PositionUpdated(DomainEvent):
    symbol: str
    timestamp: datetime
    position_qty: float
    average_price: float

    EVENT_TYPE: ClassVar[str] = "position.updated"


@dataclass(frozen=True, slots=True)
class PortfolioValuated(DomainEvent):
    timestamp: datetime
    nav: float
    cash: float
    marks: dict[str, float] = field(default_factory=dict)

    EVENT_TYPE: ClassVar[str] = "portfolio.valuated"
