from __future__ import annotations

from datetime import datetime, timezone

from quantlab.core.events import FeatureCalculated, MarketDataArrived
from quantlab.domain.research.handlers.feature_handler import FeatureCalculationHandler
from quantlab.domain.research.handlers.signal_handler import SignalGenerationHandler
from quantlab.infra.bus import (
    ExceptionMiddleware,
    InMemoryEventBus,
    LoggingMiddleware,
    SubscriptionRegistry,
    TimingMiddleware,
)


def build_bus() -> InMemoryEventBus:
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
    signal_handler = SignalGenerationHandler(bus, threshold=10000.0)

    bus.subscribe(MarketDataArrived, feature_handler)
    bus.subscribe(FeatureCalculated, signal_handler)

    return bus


def demo_run() -> None:
    bus = build_bus()

    event = MarketDataArrived(
        symbol="BTCUSDT",
        timestamp=datetime.now(timezone.utc),
        last_price=100.0,
        volume=150.0,
        source="backtest.replay",
    )

    bus.publish(event)


if __name__ == "__main__":
    demo_run()
