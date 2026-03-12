from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True, kw_only=True)
class DomainEvent:
    event_id: str = field(default_factory=lambda: str(uuid4()))
    occurred_at: datetime = field(default_factory=utc_now)
    source: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    EVENT_TYPE: ClassVar[str] = "domain.event"

    @property
    def event_type(self) -> str:
        return self.EVENT_TYPE

    @classmethod
    def event_name(cls) -> str:
        return cls.EVENT_TYPE

@dataclass(frozen=True, slots=True)
class JobQueued(DomainEvent):
    job_id: str

    EVENT_TYPE: ClassVar[str] = "job.queued"


@dataclass(frozen=True, slots=True)
class JobStarted(DomainEvent):
    job_id: str

    EVENT_TYPE: ClassVar[str] = "job.started"


@dataclass(frozen=True, slots=True)
class JobProgressed(DomainEvent):
    job_id: str
    progress: float
    message: str = ""

    EVENT_TYPE: ClassVar[str] = "job.progressed"


@dataclass(frozen=True, slots=True)
class JobSucceeded(DomainEvent):
    job_id: str
    result: dict[str, Any] | None = None

    EVENT_TYPE: ClassVar[str] = "job.succeeded"


@dataclass(frozen=True, slots=True)
class JobFailed(DomainEvent):
    job_id: str
    error: str

    EVENT_TYPE: ClassVar[str] = "job.failed"


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

