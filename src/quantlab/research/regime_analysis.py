from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from statistics import fmean, pstdev

from quantlab.core.models import Bar
from quantlab.research.factor_evaluation import FactorEvaluator, build_forward_returns
from quantlab.research.factors import FactorExposure
from quantlab.research.regime import (
    RegimeConditionedDecayPoint,
    RegimeConditionedFactorSummary,
    RegimeDurationSummary,
    RegimeInferenceResult,
    RegimeObservationFrame,
    RegimeProfile,
)


class RegimeAnalyzer:
    def duration_summaries(self, inference: RegimeInferenceResult) -> tuple[RegimeDurationSummary, ...]:
        runs: dict[int, list[int]] = defaultdict(list)
        if not inference.states:
            return ()
        current_state = inference.states[0].state_id
        current_length = 1
        for estimate in inference.states[1:]:
            if estimate.state_id == current_state:
                current_length += 1
                continue
            runs[current_state].append(current_length)
            current_state = estimate.state_id
            current_length = 1
        runs[current_state].append(current_length)
        return tuple(
            RegimeDurationSummary(
                scope=inference.scope,
                state_id=state_id,
                episodes=len(lengths),
                mean_duration=fmean(lengths),
                max_duration=max(lengths),
                min_duration=min(lengths),
            )
            for state_id, lengths in sorted(runs.items())
        )

    def profiles(
        self,
        frame: RegimeObservationFrame,
        inference: RegimeInferenceResult,
        bars: Sequence[Bar] = (),
    ) -> tuple[RegimeProfile, ...]:
        observation_lookup = {observation.as_of: observation for observation in frame.observations}
        market_profiles = _market_bar_profiles(bars)
        by_state: dict[int, list[dict[str, float]]] = defaultdict(list)
        market_returns: dict[int, list[float]] = defaultdict(list)
        market_vols: dict[int, list[float]] = defaultdict(list)
        for estimate in inference.states:
            observation = observation_lookup.get(estimate.as_of)
            if observation is None:
                continue
            by_state[estimate.state_id].append(dict(observation.values))
            if estimate.as_of in market_profiles:
                market_profile = market_profiles[estimate.as_of]
                if market_profile["market_return"] is not None:
                    market_returns[estimate.state_id].append(market_profile["market_return"])
                if market_profile["market_volatility"] is not None:
                    market_vols[estimate.state_id].append(market_profile["market_volatility"])
        profiles: list[RegimeProfile] = []
        for state_id, rows in sorted(by_state.items()):
            feature_names = sorted({name for row in rows for name in row})
            means = {
                feature_name: fmean(row.get(feature_name, 0.0) for row in rows)
                for feature_name in feature_names
            }
            stdevs = {
                feature_name: pstdev([row.get(feature_name, 0.0) for row in rows]) if len(rows) > 1 else 0.0
                for feature_name in feature_names
            }
            profiles.append(
                RegimeProfile(
                    scope=inference.scope,
                    state_id=state_id,
                    observations=len(rows),
                    feature_means=means,
                    feature_stdevs=stdevs,
                    mean_market_return=fmean(market_returns[state_id]) if market_returns[state_id] else None,
                    mean_market_volatility=fmean(market_vols[state_id]) if market_vols[state_id] else None,
                )
            )
        return tuple(profiles)


class RegimeConditionedFactorEvaluator:
    def __init__(self, evaluator: FactorEvaluator | None = None) -> None:
        self._evaluator = evaluator or FactorEvaluator()

    def evaluate(
        self,
        exposures: Sequence[FactorExposure],
        bars: Sequence[Bar],
        inference: RegimeInferenceResult,
        *,
        decay_horizons: Sequence[int] = (1, 3, 5),
    ) -> tuple[tuple[RegimeConditionedFactorSummary, ...], tuple[RegimeConditionedDecayPoint, ...]]:
        if not exposures or not inference.states:
            return (), ()
        timestamps_by_state = _timestamps_by_state(inference)
        forward_returns = build_forward_returns(bars, horizon=1)
        dollar_volume = {
            (bar.instrument.symbol, bar.timestamp): bar.close * bar.volume
            for bar in bars
        }
        summaries: list[RegimeConditionedFactorSummary] = []
        decay_points: list[RegimeConditionedDecayPoint] = []
        for state_id, timestamps in sorted(timestamps_by_state.items()):
            filtered_exposures = tuple(
                exposure
                for exposure in exposures
                if exposure.as_of in timestamps
            )
            cross_sections = self._evaluator.cross_sections(filtered_exposures, forward_returns)
            for summary in self._evaluator.summarize(cross_sections):
                capacity = _capacity_proxy(summary.factor_name, cross_sections, dollar_volume)
                summaries.append(
                    RegimeConditionedFactorSummary(
                        scope=inference.scope,
                        state_id=state_id,
                        factor_name=summary.factor_name,
                        observations=summary.observations,
                        cross_sections=summary.cross_sections,
                        mean_ic=summary.mean_ic,
                        mean_rank_ic=summary.mean_rank_ic,
                        mean_quantile_spread=summary.mean_quantile_spread,
                        hit_rate=summary.hit_rate,
                        mean_turnover=summary.mean_top_quantile_turnover,
                        mean_capacity_proxy=capacity,
                    )
                )
            for point in self._evaluator.decay(filtered_exposures, bars, decay_horizons):
                decay_points.append(
                    RegimeConditionedDecayPoint(
                        scope=inference.scope,
                        state_id=state_id,
                        factor_name=point.factor_name,
                        horizon=point.horizon,
                        cross_sections=point.cross_sections,
                        mean_ic=point.mean_ic,
                        mean_rank_ic=point.mean_rank_ic,
                    )
                )
        return tuple(summaries), tuple(decay_points)


def _timestamps_by_state(inference: RegimeInferenceResult) -> dict[int, set[object]]:
    by_state: dict[int, set[object]] = defaultdict(set)
    for estimate in inference.states:
        by_state[estimate.state_id].add(estimate.as_of)
    return by_state


def _market_bar_profiles(bars: Sequence[Bar]) -> dict[object, dict[str, float | None]]:
    if not bars:
        return {}
    by_symbol: dict[str, list[Bar]] = defaultdict(list)
    for bar in sorted(bars, key=lambda item: (item.instrument.symbol, item.timestamp)):
        by_symbol[bar.instrument.symbol].append(bar)
    profiles: dict[object, dict[str, float | None]] = defaultdict(lambda: {"market_return": None, "market_volatility": None})
    per_timestamp_returns: dict[object, list[float]] = defaultdict(list)
    for symbol_bars in by_symbol.values():
        for index in range(1, len(symbol_bars)):
            previous = symbol_bars[index - 1]
            current = symbol_bars[index]
            if previous.close == 0:
                continue
            per_timestamp_returns[current.timestamp].append((current.close / previous.close) - 1.0)
    for as_of, returns in per_timestamp_returns.items():
        profiles[as_of] = {
            "market_return": fmean(returns) if returns else None,
            "market_volatility": pstdev(returns) if len(returns) > 1 else 0.0 if returns else None,
        }
    return profiles


def _capacity_proxy(
    factor_name: str,
    cross_sections,
    dollar_volume: Mapping[tuple[str, object], float],
) -> float | None:
    capacity_observations: list[float] = []
    for cross_section in cross_sections:
        if cross_section.factor_name != factor_name:
            continue
        liquidities = [
            dollar_volume[(symbol, cross_section.as_of)]
            for symbol in cross_section.top_quantile_symbols
            if (symbol, cross_section.as_of) in dollar_volume
        ]
        if liquidities:
            capacity_observations.append(fmean(liquidities))
    return fmean(capacity_observations) if capacity_observations else None
