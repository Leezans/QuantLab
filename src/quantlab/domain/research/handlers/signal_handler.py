from __future__ import annotations

from quantlab.core.events import DomainEvent, FeatureCalculated, SignalGenerated
from quantlab.core.interfaces import EventBus, EventHandler


class SignalGenerationHandler(EventHandler):
    def __init__(self, bus: EventBus, threshold: float = 10000.0) -> None:
        self._bus = bus
        self._threshold = threshold

    def __call__(self, event: DomainEvent) -> None:
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
            correlation_id=event.correlation_id or event.event_id,
            causation_id=event.event_id,
        )

        self._bus.publish(next_event)
