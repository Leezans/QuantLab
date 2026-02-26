from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from cLab.core.domain.errors import ValidationError

OrderSide = Literal["buy", "sell"]


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValidationError(f"end < start: {self.end.isoformat()} < {self.start.isoformat()}")


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class Fill:
    timestamp: datetime
    side: OrderSide
    quantity: float
    price: float
    fee: float


@dataclass(frozen=True)
class StrategyParams:
    fast_window: int
    slow_window: int

    def __post_init__(self) -> None:
        if self.fast_window <= 0 or self.slow_window <= 0:
            raise ValidationError("strategy windows must be positive")
        if self.fast_window >= self.slow_window:
            raise ValidationError("fast_window must be less than slow_window")


@dataclass(frozen=True)
class BacktestMetrics:
    total_return: float
    max_drawdown: float
    sharpe_ratio: float
    final_equity: float
    trade_count: int


@dataclass(frozen=True)
class BacktestResult:
    metrics: BacktestMetrics
    equity_curve: list[float]
    fills: list[Fill]


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    symbol: str
    date_range: DateRange
    strategy_params: StrategyParams
    fee_bps: float
    slippage_bps: float
    initial_cash: float
    seed: int | None
    metrics: BacktestMetrics
    artifact_path: str
    created_at: datetime
