from quantlab.core.events import DomainEvent
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar



@dataclass(frozen=True, slots=True)
class FeatureCalculated(DomainEvent):
    symbol: str
    timestamp: datetime
    feature_name: str
    feature_value: float

    EVENT_TYPE: ClassVar[str] = "feature.calculated"