from __future__ import annotations

from quantlab.core.events import FeatureCalculated, MarketDataArrived
from quantlab.infra.bus.interfaces import EventBus, EventHandler
from quantlab.infra.bus.types import EventEnvelope


class FeatureCalculationHandler(EventHandler):
    def __init__(self, bus: EventBus) -> None:
        self._bus = bus

    def __call__(self, envelope: EventEnvelope) -> None:
        event = envelope.payload
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
        )

        self._bus.publish(EventEnvelope.wrap(
            next_event,
            correlation_id=envelope.correlation_id or envelope.event_id,
            causation_id=envelope.event_id,
        ))