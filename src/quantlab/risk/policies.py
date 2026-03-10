from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from quantlab.core.models import PortfolioSnapshot, TargetPosition


class MaxPositionWeightPolicy:
    def __init__(self, limit: float) -> None:
        self._limit = abs(limit)

    def apply(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
    ) -> tuple[TargetPosition, ...]:
        del portfolio
        return tuple(
            TargetPosition(
                as_of=target.as_of,
                instrument=target.instrument,
                target_weight=max(-self._limit, min(self._limit, target.target_weight)),
                reason=target.reason,
                signal_name=target.signal_name,
                metadata=target.metadata,
            )
            for target in targets
        )


class MaxGrossExposurePolicy:
    def __init__(self, limit: float) -> None:
        self._limit = abs(limit)

    def apply(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
    ) -> tuple[TargetPosition, ...]:
        del portfolio
        gross = sum(abs(target.target_weight) for target in targets)
        if gross <= self._limit or gross == 0:
            return tuple(targets)
        scale = self._limit / gross
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


class LiquidityParticipationPolicy:
    def __init__(self, max_adv_fraction: float = 0.10, liquidity_key: str = "adv_notional") -> None:
        self._max_adv_fraction = abs(max_adv_fraction)
        self._liquidity_key = liquidity_key

    def apply(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
    ) -> tuple[TargetPosition, ...]:
        nav = _portfolio_nav(portfolio, targets)
        adjusted: list[TargetPosition] = []
        for target in targets:
            liquidity_notional = max(
                _metadata_float(target.metadata, self._liquidity_key, 0.0),
                _metadata_float(target.metadata, "proxy_liquidity_score", 0.0),
            )
            capped_weight = target.target_weight
            if liquidity_notional > 0.0 and nav > 0.0 and self._max_adv_fraction > 0.0:
                max_weight = (liquidity_notional * self._max_adv_fraction) / nav
                capped_weight = max(-max_weight, min(max_weight, capped_weight))
            adjusted.append(
                TargetPosition(
                    as_of=target.as_of,
                    instrument=target.instrument,
                    target_weight=capped_weight,
                    reason=target.reason,
                    signal_name=target.signal_name,
                    metadata=target.metadata,
                )
            )
        return tuple(adjusted)


class RegimeLike(Protocol):
    state_id: int


class RegimeStateLimitPolicy:
    def __init__(
        self,
        *,
        max_position_weight_by_state: Mapping[int, float] | None = None,
        max_gross_by_state: Mapping[int, float] | None = None,
    ) -> None:
        self._max_position_weight_by_state = {
            int(state_id): abs(float(limit))
            for state_id, limit in (max_position_weight_by_state or {}).items()
        }
        self._max_gross_by_state = {
            int(state_id): abs(float(limit))
            for state_id, limit in (max_gross_by_state or {}).items()
        }

    def apply_with_regime(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
        regime_signal: RegimeLike | None = None,
    ) -> tuple[TargetPosition, ...]:
        if regime_signal is None:
            return tuple(targets)
        current = tuple(targets)
        position_limit = self._max_position_weight_by_state.get(regime_signal.state_id)
        if position_limit is not None:
            current = MaxPositionWeightPolicy(position_limit).apply(current, portfolio)
        gross_limit = self._max_gross_by_state.get(regime_signal.state_id)
        if gross_limit is not None:
            current = MaxGrossExposurePolicy(gross_limit).apply(current, portfolio)
        return tuple(
            TargetPosition(
                as_of=target.as_of,
                instrument=target.instrument,
                target_weight=target.target_weight,
                reason=target.reason,
                signal_name=target.signal_name,
                metadata={**dict(target.metadata), "regime_state": str(regime_signal.state_id)},
            )
            for target in current
        )


class RiskPolicyStack:
    def __init__(self, policies: Sequence[object]) -> None:
        self._policies = tuple(policies)

    def apply(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
        regime_signal: RegimeLike | None = None,
    ) -> tuple[TargetPosition, ...]:
        current = tuple(targets)
        for policy in self._policies:
            if regime_signal is not None and hasattr(policy, "apply_with_regime"):
                current = tuple(policy.apply_with_regime(current, portfolio, regime_signal))
                continue
            current = tuple(policy.apply(current, portfolio))
        return current


def _portfolio_nav(portfolio: PortfolioSnapshot, targets: Sequence[TargetPosition]) -> float:
    marks = {
        target.instrument.symbol: _metadata_float(target.metadata, "mark_price", 0.0)
        for target in targets
        if _metadata_float(target.metadata, "mark_price", 0.0) > 0.0
    }
    nav = portfolio.nav(marks) if portfolio.positions else portfolio.cash
    return nav if nav > 0.0 else max(portfolio.cash, 1.0)


def _metadata_float(metadata: Mapping[str, object], key: str, default: float) -> float:
    value = metadata.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)
