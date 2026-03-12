from __future__ import annotations

from quantlab.core.events import DomainEvent
from quantlab.domain.data.events import MarketDataArrived
from quantlab.domain.research.events import FeatureCalculated
from quantlab.core.interfaces import EventBus, EventHandler


class FeatureCalculationHandler(EventHandler):
    def __init__(self, bus: EventBus) -> None:
        self._bus = bus

    def __call__(self, event: DomainEvent) -> None:
        if not isinstance(event, MarketDataArrived):
            return

        # demo feature
        feature_value = event.last_price * event.volume

        next_event = FeatureCalculated(
            symbol=event.symbol,
            timestamp=event.timestamp,
            feature_name="price_x_volume",
            feature_value=feature_value,
            source="research.feature_engine",
            correlation_id=event.correlation_id or event.event_id,
            causation_id=event.event_id,
        )

        self._bus.publish(next_event)
