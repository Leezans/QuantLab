from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from quantlab.backtest.costs import FixedBpsTransactionCostModel
from quantlab.backtest.metrics import PerformanceMetrics, compute_metrics
from quantlab.core.enums import Side
from quantlab.core.models import Bar, Fill, Order, PortfolioSnapshot, Position, Signal
from quantlab.execution.algorithms import ImmediateExecutionAlgorithm
from quantlab.portfolio.construction import GrossExposureAllocator
from quantlab.risk.policies import RiskPolicyStack
from quantlab.strategy.base import SignalWeightedStrategy


@dataclass(frozen=True, slots=True)
class BacktestResult:
    report: PerformanceMetrics
    orders: tuple[Order, ...]
    fills: tuple[Fill, ...]
    equity_curve: tuple[float, ...]
    final_portfolio: PortfolioSnapshot


class SimpleBacktestEngine:
    def __init__(
        self,
        strategy: SignalWeightedStrategy,
        allocator: GrossExposureAllocator,
        risk_policy: RiskPolicyStack,
        execution_algorithm: ImmediateExecutionAlgorithm,
        cost_model: FixedBpsTransactionCostModel,
        initial_cash: float = 1_000_000.0,
    ) -> None:
        self._strategy = strategy
        self._allocator = allocator
        self._risk_policy = risk_policy
        self._execution_algorithm = execution_algorithm
        self._cost_model = cost_model
        self._initial_cash = initial_cash

    def run(self, bars: Sequence[Bar], signals: Sequence[Signal]) -> BacktestResult:
        if not bars:
            empty_portfolio = PortfolioSnapshot(timestamp=datetime.now(timezone.utc), cash=self._initial_cash)
            return BacktestResult(
                report=compute_metrics((), turnover=0.0),
                orders=(),
                fills=(),
                equity_curve=(),
                final_portfolio=empty_portfolio,
            )

        bars_by_timestamp: dict[datetime, dict[str, Bar]] = defaultdict(dict)
        for bar in bars:
            bars_by_timestamp[bar.timestamp][bar.instrument.symbol] = bar

        signals_by_timestamp: dict[datetime, list[Signal]] = defaultdict(list)
        for signal in signals:
            signals_by_timestamp[signal.as_of].append(signal)

        timestamps = sorted(bars_by_timestamp)
        positions: dict[str, Position] = {}
        portfolio = PortfolioSnapshot(timestamp=timestamps[0], cash=self._initial_cash, positions=positions)
        marks: dict[str, float] = {}
        orders: list[Order] = []
        fills: list[Fill] = []
        equity_curve: list[float] = []
        turnover_notional = 0.0

        for timestamp in timestamps:
            market_slice = bars_by_timestamp[timestamp]
            marks.update({symbol: bar.close for symbol, bar in market_slice.items()})
            portfolio.timestamp = timestamp

            pending_signals = tuple(signals_by_timestamp.get(timestamp, ()))
            if pending_signals:
                targets = self._strategy.generate_targets(pending_signals, portfolio)
                allocated = self._allocator.allocate(targets, portfolio)
                approved = self._risk_policy.apply(allocated, portfolio)
                generated_orders = self._execution_algorithm.create_orders(approved, portfolio, marks)
                for order in generated_orders:
                    price = marks.get(order.instrument.symbol)
                    if price is None:
                        continue
                    fee = self._cost_model.estimate(order, price)
                    signed_quantity = order.quantity if order.side is Side.BUY else -order.quantity
                    position = positions.setdefault(order.instrument.symbol, Position(instrument=order.instrument))
                    position.quantity += signed_quantity
                    position.average_price = price if position.quantity != 0 else 0.0
                    portfolio.cash -= signed_quantity * price + fee
                    turnover_notional += abs(signed_quantity * price)
                    orders.append(order)
                    fills.append(
                        Fill(
                            order_id=order.order_id,
                            timestamp=timestamp,
                            instrument=order.instrument,
                            side=order.side,
                            quantity=order.quantity,
                            price=price,
                            fees=fee,
                        )
                    )

            equity_curve.append(portfolio.nav(marks))

        report = compute_metrics(
            tuple(equity_curve),
            turnover=0.0 if self._initial_cash == 0 else turnover_notional / self._initial_cash,
        )
        return BacktestResult(
            report=report,
            orders=tuple(orders),
            fills=tuple(fills),
            equity_curve=tuple(equity_curve),
            final_portfolio=portfolio,
        )

