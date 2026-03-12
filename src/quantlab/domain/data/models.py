from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional

from quantlab.domain.data.enums import AssetClass, OrderType, Side, SignalDirection


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
    price: float
    quantity: float
    trade_id: Optional[str|int] = None
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
class OrderBookLevel:  # 订单簿某一档（任意深度 level）
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
class FeatureVector:   # 某个时刻、某个交易标的的一组因子值（feature vector）
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

    # Net Asset Value（资产净值）nav
    def nav(self, marketPrices: Mapping[str, float]) -> float:
        return self.cash + sum(
            position.quantity * marketPrices.get(symbol, position.average_price)
            for symbol, position in self.positions.items()
        )

    # 按照金融行业惯例，通常将投资组合的总风险暴露称为“Gross Exposure”。
    def gross_exposure(self, marketPrices: Mapping[str, float]) -> float:
        return sum(
            abs(position.quantity * marketPrices.get(symbol, position.average_price))
            for symbol, position in self.positions.items()
        )
    
     # 净风险暴露（Net Exposure）是指投资组合中多头和空头仓位的净值，反映了投资组合相对于市场的整体风险暴露程度。
     # 计算方法是将每个持仓的数量乘以其当前价格（或标记价格），然后根据持仓方向（多头为正，空头为负）进行加总。
     # 净风险暴露可以帮助投资者了解他们的投资组合在市场波动中的潜在风险。
    def net_exposure(self, marketPrices: Mapping[str, float]) -> float:
        return sum(
            position.quantity * marketPrices.get(symbol, position.average_price)
            for symbol, position in self.positions.items()
        )




if __name__ == "__main__":
    # 简单的测试用例
    btc = Instrument(symbol="BTC-USD", venue="Coinbase", asset_class=AssetClass.CRYPTO)
    gold = Instrument(symbol="GOLD", venue="COMEX", asset_class=AssetClass.FUTURE)
    ps = PortfolioSnapshot(
    timestamp=datetime.now(),
    cash=1000.0,
    positions={'BTC-USD': Position(instrument=btc, quantity=2, average_price=120.0),
               'GOLD': Position(instrument=gold, quantity=5, average_price=1800.0)}
)
    print(ps.nav({'BTC-USD': 150.0, 'GOLD': 1850.0}))  
    print(ps.gross_exposure({'BTC-USD': 150.0, 'GOLD': 1850.0}))
    print(ps.net_exposure({'BTC-USD': 150.0, 'GOLD': 1850.0}))