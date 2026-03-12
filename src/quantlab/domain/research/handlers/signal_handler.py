from __future__ import annotations

from quantlab.core.events import DomainEvent
from quantlab.domain.research.events import FeatureCalculated
from quantlab.domain.data.events import MarketDataArrived

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

        next_event = None

        self._bus.publish(next_event)
