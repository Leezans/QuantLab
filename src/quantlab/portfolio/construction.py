from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from quantlab.core.models import PortfolioSnapshot, TargetPosition


class GrossExposureAllocator:
    def __init__(self, gross_target: float = 1.0) -> None:
        self._gross_target = gross_target

    @property
    def gross_target(self) -> float:
        return self._gross_target

    def with_gross_target(self, gross_target: float) -> "GrossExposureAllocator":
        return GrossExposureAllocator(gross_target=gross_target)

    def allocate(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
    ) -> tuple[TargetPosition, ...]:
        del portfolio
        gross = sum(abs(target.target_weight) for target in targets)
        if gross == 0:
            return tuple(targets)
        scale = self._gross_target / gross
        return tuple(
            TargetPosition(
                as_of=target.as_of,
                instrument=target.instrument,
                target_weight=target.target_weight * scale,
                reason=target.reason,
                signal_name=target.signal_name,
                metadata=target.metadata,
            )
            for target in targets
        )


class LiquidityAwareAllocator:
    def __init__(
        self,
        gross_target: float = 1.0,
        max_abs_weight: float = 0.25,
        max_turnover: float = 1.0,
        max_adv_fraction: float = 0.10,
        min_liquidity_score: float = 0.0,
        cost_penalty_bps: float = 25.0,
    ) -> None:
        self._gross_target = abs(gross_target)
        self._max_abs_weight = abs(max_abs_weight)
        self._max_turnover = abs(max_turnover)
        self._max_adv_fraction = abs(max_adv_fraction)
        self._min_liquidity_score = max(0.0, min_liquidity_score)
        self._cost_penalty_bps = max(0.0, cost_penalty_bps)

    @property
    def gross_target(self) -> float:
        return self._gross_target

    def with_gross_target(self, gross_target: float) -> "LiquidityAwareAllocator":
        return LiquidityAwareAllocator(
            gross_target=gross_target,
            max_abs_weight=self._max_abs_weight,
            max_turnover=self._max_turnover,
            max_adv_fraction=self._max_adv_fraction,
            min_liquidity_score=self._min_liquidity_score,
            cost_penalty_bps=self._cost_penalty_bps,
        )

    def allocate(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
    ) -> tuple[TargetPosition, ...]:
        nav = _portfolio_nav(portfolio, targets)
        current_weights = _current_weights(portfolio, targets, nav)
        constrained: list[TargetPosition] = []
        for target in targets:
            metadata = dict(target.metadata)
            desired_weight = max(-self._max_abs_weight, min(self._max_abs_weight, target.target_weight))

            liquidity_score = _metadata_float(metadata, "liquidity_score", 1.0)
            if liquidity_score < self._min_liquidity_score and self._min_liquidity_score > 0.0:
                desired_weight *= max(liquidity_score, 0.0) / self._min_liquidity_score

            adv_notional = max(
                _metadata_float(metadata, "adv_notional", 0.0),
                _metadata_float(metadata, "proxy_liquidity_score", 0.0),
            )
            if adv_notional > 0.0 and nav > 0.0 and self._max_adv_fraction > 0.0:
                max_weight_from_adv = (adv_notional * self._max_adv_fraction) / nav
                desired_weight = max(-max_weight_from_adv, min(max_weight_from_adv, desired_weight))

            expected_cost_bps = max(_metadata_float(metadata, "expected_cost_bps", 0.0), 0.0)
            if self._cost_penalty_bps > 0.0 and expected_cost_bps > 0.0:
                desired_weight /= 1.0 + (expected_cost_bps / self._cost_penalty_bps)

            constrained.append(
                TargetPosition(
                    as_of=target.as_of,
                    instrument=target.instrument,
                    target_weight=desired_weight,
                    reason=target.reason,
                    signal_name=target.signal_name,
                    metadata=metadata,
                )
            )

        if self._max_turnover > 0.0:
            constrained = list(_apply_turnover_limit(constrained, current_weights, self._max_turnover))
        return GrossExposureAllocator(self._gross_target).allocate(constrained, portfolio)


class RegimeLike(Protocol):
    state_id: int


class RegimeAwareAllocator:
    def __init__(
        self,
        allocator: GrossExposureAllocator | LiquidityAwareAllocator,
        gross_target_by_state: Mapping[int, float],
    ) -> None:
        self._allocator = allocator
        self._gross_target_by_state = {int(state_id): abs(float(target)) for state_id, target in gross_target_by_state.items()}

    def allocate(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
        regime_signal: RegimeLike | None = None,
    ) -> tuple[TargetPosition, ...]:
        if regime_signal is None:
            return self._allocator.allocate(targets, portfolio)

        gross_target = self._gross_target_by_state.get(regime_signal.state_id, self._allocator.gross_target)
        scoped_allocator = self._allocator.with_gross_target(gross_target)
        allocated = scoped_allocator.allocate(targets, portfolio)
        return tuple(
            TargetPosition(
                as_of=target.as_of,
                instrument=target.instrument,
                target_weight=target.target_weight,
                reason=target.reason,
                signal_name=target.signal_name,
                metadata={**dict(target.metadata), "regime_state": str(regime_signal.state_id)},
            )
            for target in allocated
        )


def _portfolio_nav(portfolio: PortfolioSnapshot, targets: Sequence[TargetPosition]) -> float:
    marks = {
        target.instrument.symbol: _metadata_float(target.metadata, "mark_price", 0.0)
        for target in targets
        if _metadata_float(target.metadata, "mark_price", 0.0) > 0.0
    }
    nav = portfolio.nav(marks) if portfolio.positions else portfolio.cash
    return nav if nav > 0.0 else max(portfolio.cash, 1.0)


def _current_weights(
    portfolio: PortfolioSnapshot,
    targets: Sequence[TargetPosition],
    nav: float,
) -> dict[str, float]:
    if nav <= 0.0:
        return {}
    target_marks = {
        target.instrument.symbol: _metadata_float(target.metadata, "mark_price", 0.0)
        for target in targets
    }
    weights: dict[str, float] = {}
    for symbol, position in portfolio.positions.items():
        mark = target_marks.get(symbol) or position.average_price
        if mark <= 0.0:
            continue
        weights[symbol] = (position.quantity * mark) / nav
    return weights


def _apply_turnover_limit(
    targets: Sequence[TargetPosition],
    current_weights: Mapping[str, float],
    max_turnover: float,
) -> tuple[TargetPosition, ...]:
    deltas = {
        target.instrument.symbol: target.target_weight - current_weights.get(target.instrument.symbol, 0.0)
        for target in targets
    }
    turnover = sum(abs(delta) for delta in deltas.values())
    if turnover <= max_turnover or turnover <= 1e-12:
        return tuple(targets)
    scale = max_turnover / turnover
    return tuple(
        TargetPosition(
            as_of=target.as_of,
            instrument=target.instrument,
            target_weight=current_weights.get(target.instrument.symbol, 0.0) + (deltas[target.instrument.symbol] * scale),
            reason=target.reason,
            signal_name=target.signal_name,
            metadata=target.metadata,
        )
        for target in targets
    )


def _metadata_float(metadata: Mapping[str, object], key: str, default: float) -> float:
    value = metadata.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)
