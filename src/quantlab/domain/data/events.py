from quantlab.core.events import DomainEvent
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

@dataclass(frozen=True, slots=True)
class MarketDataArrived(DomainEvent):
    symbol: str
    timestamp: datetime
    last_price: float
    volume: float

    EVENT_TYPE: ClassVar[str] = "market_data.arrived"


