from __future__ import annotations

from quantlab.core.events import FeatureCalculated, SignalGenerated
from quantlab.infra.bus.interfaces import EventBus, EventHandler
from quantlab.infra.bus.types import EventEnvelope


class SignalGenerationHandler(EventHandler):
    def __init__(self, bus: EventBus, threshold: float = 10000.0) -> None:
        self._bus = bus
        self._threshold = threshold

    def __call__(self, envelope: EventEnvelope) -> None:
        event = envelope.payload
        if not isinstance(event, FeatureCalculated):
            return

        side = "BUY" if event.feature_value >= self._threshold else "FLAT"
        strength = min(event.feature_value / self._threshold, 2.0)

        next_event = SignalGenerated(
            symbol=event.symbol,
            timestamp=event.timestamp,
            side=side,
            strength=strength,
            reason=f"{event.feature_name}>={self._threshold}",
            source="research.signal_engine",
        )

        self._bus.publish(EventEnvelope.wrap(
            next_event,
            correlation_id=envelope.correlation_id or envelope.event_id,
            causation_id=envelope.event_id,
        ))