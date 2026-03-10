from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional

from quantlab.core.enums import AssetClass, OrderType, Side, SignalDirection


@dataclass(frozen=True, slots=True)
class Instrument:            # 交易标的（描述一个可交易的资产 如apple的stock或者黄金期货）
    symbol: str              # 交易标的的唯一标识符，通常是一个字符串，例如 "AAPL"、"BTC-USD"、"ESZ9" 等
    venue: str               # 交易所或市场，例如 "NYSE"、"NASDAQ"、"CME"、"Binance" 等
    asset_class: AssetClass  # 资产类别，例如股票、期货、加密货币等
    quote_currency: str = "USD"   # 计价货币，例如 "USD"、"EUR"、"JPY" 等
    metadata: Mapping[str, Any] = field(default_factory=dict)   # 其他可选的元数据字段，例如合约规格、交易时间等


@dataclass(frozen=True, slots=True)
class Bar:
    timestamp: datetime
    instrument: Instrument
    open: float
    high: float
    low: float
    close: float
    volume: float
    metadata: Mapping[str, Any] = field(default_factory=dict)   # 其他可选的元数据字段，例如成交量加权平均价格等


@dataclass(frozen=True, slots=True)
class Trade:
    timestamp: datetime
    instrument: Instrument
    trade_id: Optional[str|int] = None
    price: float
    quantity: float
    side: Side | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def notional(self) -> float:
        return self.price * self.quantity


@dataclass(frozen=True, slots=True)
class Quote:                    # 最佳买卖报价（Top of Book / BBO）
    timestamp: datetime
    instrument: Instrument
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    side: Side
    level: int
    price: float
    quantity: float


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    timestamp: datetime
    instrument: Instrument
    sequence_id: str
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FeatureVector:
    as_of: datetime
    instrument: Instrument
    values: Mapping[str, float]


@dataclass(frozen=True, slots=True)
class Signal:
    as_of: datetime
    instrument: Instrument
    name: str
    value: float
    direction: SignalDirection
    confidence: float = 1.0
    horizon: str = "1d"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TargetPosition:
    as_of: datetime
    instrument: Instrument
    target_weight: float
    reason: str
    signal_name: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Order:
    order_id: str
    timestamp: datetime
    instrument: Instrument
    side: Side
    quantity: float
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Fill:
    order_id: str
    timestamp: datetime
    instrument: Instrument
    side: Side
    quantity: float
    price: float
    fees: float = 0.0


@dataclass(slots=True)
class Position:
    instrument: Instrument
    quantity: float = 0.0
    average_price: float = 0.0


@dataclass(slots=True)
class PortfolioSnapshot:
    timestamp: datetime
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)

    def nav(self, marks: Mapping[str, float]) -> float:
        return self.cash + sum(
            position.quantity * marks.get(symbol, position.average_price)
            for symbol, position in self.positions.items()
        )

    def gross_exposure(self, marks: Mapping[str, float]) -> float:
        return sum(
            abs(position.quantity * marks.get(symbol, position.average_price))
            for symbol, position in self.positions.items()
        )

    def net_exposure(self, marks: Mapping[str, float]) -> float:
        return sum(
            position.quantity * marks.get(symbol, position.average_price)
            for symbol, position in self.positions.items()
        )
