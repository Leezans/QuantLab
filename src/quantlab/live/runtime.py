from __future__ import annotations

from typing import Mapping, Sequence

from quantlab.core.models import Order, PortfolioSnapshot, Signal
from quantlab.execution.algorithms import ImmediateExecutionAlgorithm
from quantlab.execution.router import OrderRouter
from quantlab.monitoring.service import MonitoringService
from quantlab.portfolio.construction import GrossExposureAllocator
from quantlab.risk.policies import RiskPolicyStack
from quantlab.strategy.base import SignalWeightedStrategy


class LiveTradingRuntime:
    def __init__(self, router: OrderRouter, monitoring: MonitoringService) -> None:
        self._router = router
        self._monitoring = monitoring

    def process_signals(
        self,
        signals: Sequence[Signal],
        portfolio: PortfolioSnapshot,
        marks: Mapping[str, float],
        strategy: SignalWeightedStrategy,
        allocator: GrossExposureAllocator,
        risk_policy: RiskPolicyStack,
        execution_algorithm: ImmediateExecutionAlgorithm,
    ) -> tuple[Order, ...]:
        targets = strategy.generate_targets(signals, portfolio)
        allocated = allocator.allocate(targets, portfolio)
        approved = risk_policy.apply(allocated, portfolio)
        routed = self._router.route(execution_algorithm.create_orders(approved, portfolio, marks))
        self._monitoring.record("live_runtime", "ok", f"routed_orders={len(routed)}")
        return routed

