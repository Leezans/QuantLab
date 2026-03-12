from __future__ import annotations
# 引导程序，负责构建事件总线、注册事件处理器，并启动示例运行
from datetime import datetime, timezone


from quantlab.core.events import MarketDataArrived
from quantlab.infra.bus import (
    ExceptionMiddleware,
    InMemoryEventBus,
    LoggingMiddleware,
    SubscriptionRegistry,
    TimingMiddleware,
)
from quantlab.infra.bus.types import EventEnvelope
from quantlab.domain.research.handlers.feature_handler import FeatureCalculationHandler
from quantlab.domain.research.handlers.signal_handler import SignalGenerationHandler


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

    # research chain
    feature_handler = FeatureCalculationHandler(bus)
    signal_handler = SignalGenerationHandler(bus, threshold=10000.0)

    # wiring: route by event_type
    bus.subscribe("MarketDataArrived", feature_handler)
    bus.subscribe("FeatureCalculated", signal_handler)

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

    bus.publish(EventEnvelope.wrap(event))


if __name__ == "__main__":
    demo_run()