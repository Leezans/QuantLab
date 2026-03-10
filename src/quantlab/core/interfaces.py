from __future__ import annotations

from datetime import datetime
from typing import Mapping, Protocol, Sequence, runtime_checkable

from quantlab.core.enums import DataFrequency
from quantlab.core.models import (
    Bar,
    FeatureVector,
    Instrument,
    Order,
    OrderBookSnapshot,
    PortfolioSnapshot,
    Quote,
    Signal,
    TargetPosition,
    Trade,
)


@runtime_checkable
class MarketDataSource(Protocol):
    def fetch_bars(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> Sequence[Bar]:
        ...


@runtime_checkable
class TradeDataSource(Protocol):
    def fetch_trades(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
    ) -> Sequence[Trade]:
        ...


@runtime_checkable
class QuoteDataSource(Protocol):
    def fetch_quotes(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> Sequence[Quote]:
        ...


@runtime_checkable
class OrderBookDataSource(Protocol):
    def fetch_order_book_snapshots(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> Sequence[OrderBookSnapshot]:
        ...


@runtime_checkable
class FeatureEngineer(Protocol):
    def build(self, bars: Sequence[Bar]) -> Sequence[FeatureVector]:
        ...


@runtime_checkable
class AlphaModel(Protocol):
    def generate(self, features: Sequence[FeatureVector]) -> Sequence[Signal]:
        ...


@runtime_checkable
class StrategyModel(Protocol):
    def generate_targets(
        self,
        signals: Sequence[Signal],
        portfolio: PortfolioSnapshot,
    ) -> Sequence[TargetPosition]:
        ...


@runtime_checkable
class PortfolioConstructor(Protocol):
    def allocate(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
    ) -> Sequence[TargetPosition]:
        ...


@runtime_checkable
class RiskPolicy(Protocol):
    def apply(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
    ) -> Sequence[TargetPosition]:
        ...


@runtime_checkable
class ExecutionAlgorithm(Protocol):
    def create_orders(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
        marks: Mapping[str, float],
    ) -> Sequence[Order]:
        ...
