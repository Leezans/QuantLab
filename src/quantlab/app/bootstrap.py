from __future__ import annotations

from quantlab.config.models import ResearchSettings
from quantlab.domain.events import FeatureCalculated, MarketDataArrived
from quantlab.domain.research.handlers.feature_handler import FeatureCalculationHandler
from quantlab.domain.research.handlers.signal_handler import SignalGenerationHandler
from quantlab.infra.bus import (
    ExceptionMiddleware,
    InMemoryEventBus,
    LoggingMiddleware,
    SubscriptionRegistry,
    TimingMiddleware,
)


def build_bus(settings: ResearchSettings) -> InMemoryEventBus:
    registry = SubscriptionRegistry()
    bus = InMemoryEventBus(
        registry=registry,
        middlewares=[
            ExceptionMiddleware(),
            LoggingMiddleware(),
            TimingMiddleware(),
        ],
    )

    feature_handler = FeatureCalculationHandler(bus)
    signal_handler = SignalGenerationHandler(bus, threshold=settings.signal_threshold)

    bus.subscribe(MarketDataArrived, feature_handler)
    bus.subscribe(FeatureCalculated, signal_handler)

    return bus
