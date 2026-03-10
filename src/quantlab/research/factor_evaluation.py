from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from statistics import fmean, pstdev

from quantlab.core.models import Bar
from quantlab.research.factors import FactorExposure


@dataclass(frozen=True, slots=True)
class FactorCrossSection:
    factor_name: str
    as_of: datetime
    observations: int
    information_coefficient: float | None
    rank_information_coefficient: float | None
    top_quantile_return: float | None
    bottom_quantile_return: float | None
    quantile_spread: float | None
    top_quantile_symbols: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FactorSummary:
    factor_name: str
    observations: int
    cross_sections: int
    mean_ic: float | None
    ic_ir: float | None
    mean_rank_ic: float | None
    rank_ic_ir: float | None
    positive_ic_ratio: float | None
    mean_quantile_spread: float | None
    hit_rate: float | None
    mean_top_quantile_turnover: float | None
    ic_autocorrelation: float | None
    rank_ic_autocorrelation: float | None


@dataclass(frozen=True, slots=True)
class FactorDecayPoint:
    factor_name: str
    horizon: int
    cross_sections: int
    mean_ic: float | None
    mean_rank_ic: float | None


class FactorEvaluator:
    def __init__(self, quantiles: int = 5, min_observations: int = 3) -> None:
        self._quantiles = quantiles
        self._min_observations = min_observations

    def evaluate(
        self,
        exposures: Sequence[FactorExposure],
        realized_returns: Mapping[tuple[str, datetime], float],
    ) -> tuple[FactorSummary, ...]:
        cross_sections = self.cross_sections(exposures, realized_returns)
        return self.summarize(cross_sections)

    def decay(
        self,
        exposures: Sequence[FactorExposure],
        bars: Sequence[Bar],
        horizons: Sequence[int],
    ) -> tuple[FactorDecayPoint, ...]:
        decay_points: list[FactorDecayPoint] = []
        unique_horizons = tuple(sorted({int(horizon) for horizon in horizons if int(horizon) > 0}))
        for horizon in unique_horizons:
            forward_returns = build_forward_returns(bars, horizon=horizon)
            grouped: dict[str, list[FactorCrossSection]] = defaultdict(list)
            for cross_section in self.cross_sections(exposures, forward_returns):
                grouped[cross_section.factor_name].append(cross_section)
            for factor_name, sections in sorted(grouped.items()):
                ic_values = [value for value in (section.information_coefficient for section in sections) if value is not None]
                rank_ic_values = [value for value in (section.rank_information_coefficient for section in sections) if value is not None]
                decay_points.append(
                    FactorDecayPoint(
                        factor_name=factor_name,
                        horizon=horizon,
                        cross_sections=len(sections),
                        mean_ic=_mean_or_none(ic_values),
                        mean_rank_ic=_mean_or_none(rank_ic_values),
                    )
                )
        return tuple(decay_points)

    def cross_sections(
        self,
        exposures: Sequence[FactorExposure],
        realized_returns: Mapping[tuple[str, datetime], float],
    ) -> tuple[FactorCrossSection, ...]:
        grouped: dict[str, dict[datetime, list[FactorExposure]]] = defaultdict(lambda: defaultdict(list))
        for exposure in exposures:
            grouped[exposure.factor_name][exposure.as_of].append(exposure)

        results: list[FactorCrossSection] = []
        for factor_name, by_date in sorted(grouped.items()):
            for as_of, dated_exposures in sorted(by_date.items()):
                paired = [
                    (exposure, realized_returns[(exposure.instrument.symbol, exposure.as_of)])
                    for exposure in dated_exposures
                    if (exposure.instrument.symbol, exposure.as_of) in realized_returns
                ]
                if len(paired) < self._min_observations:
                    continue
                exposures_only = [exposure.value for exposure, _ in paired]
                returns_only = [forward_return for _, forward_return in paired]
                sorted_pairs = sorted(paired, key=lambda item: item[0].value, reverse=True)
                bucket_size = max(1, len(sorted_pairs) // self._quantiles)
                top_bucket = sorted_pairs[:bucket_size]
                bottom_bucket = sorted_pairs[-bucket_size:]
                top_return = fmean([forward_return for _, forward_return in top_bucket])
                bottom_return = fmean([forward_return for _, forward_return in bottom_bucket])
                results.append(
                    FactorCrossSection(
                        factor_name=factor_name,
                        as_of=as_of,
                        observations=len(paired),
                        information_coefficient=_pearson(exposures_only, returns_only),
                        rank_information_coefficient=_spearman(exposures_only, returns_only),
                        top_quantile_return=top_return,
                        bottom_quantile_return=bottom_return,
                        quantile_spread=top_return - bottom_return,
                        top_quantile_symbols=tuple(sorted(exposure.instrument.symbol for exposure, _ in top_bucket)),
                    )
                )
        return tuple(results)

    def summarize(self, cross_sections: Sequence[FactorCrossSection]) -> tuple[FactorSummary, ...]:
        grouped: dict[str, list[FactorCrossSection]] = defaultdict(list)
        for cross_section in cross_sections:
            grouped[cross_section.factor_name].append(cross_section)

        summaries: list[FactorSummary] = []
        for factor_name, dated_sections in sorted(grouped.items()):
            ic_values = [value for value in (section.information_coefficient for section in dated_sections) if value is not None]
            rank_ic_values = [value for value in (section.rank_information_coefficient for section in dated_sections) if value is not None]
            spreads = [value for value in (section.quantile_spread for section in dated_sections) if value is not None]
            turnovers = _top_quantile_turnovers(dated_sections)

            summaries.append(
                FactorSummary(
                    factor_name=factor_name,
                    observations=sum(section.observations for section in dated_sections),
                    cross_sections=len(dated_sections),
                    mean_ic=_mean_or_none(ic_values),
                    ic_ir=_information_ratio(ic_values),
                    mean_rank_ic=_mean_or_none(rank_ic_values),
                    rank_ic_ir=_information_ratio(rank_ic_values),
                    positive_ic_ratio=(sum(1 for value in ic_values if value > 0) / len(ic_values)) if ic_values else None,
                    mean_quantile_spread=_mean_or_none(spreads),
                    hit_rate=(sum(1 for value in spreads if value > 0) / len(spreads)) if spreads else None,
                    mean_top_quantile_turnover=_mean_or_none(turnovers),
                    ic_autocorrelation=_lag_one_autocorrelation(ic_values),
                    rank_ic_autocorrelation=_lag_one_autocorrelation(rank_ic_values),
                )
            )
        return tuple(summaries)


def build_forward_returns(bars: Sequence[Bar], horizon: int = 1) -> dict[tuple[str, datetime], float]:
    by_symbol: dict[str, list[Bar]] = defaultdict(list)
    for bar in sorted(bars, key=lambda value: (value.instrument.symbol, value.timestamp)):
        by_symbol[bar.instrument.symbol].append(bar)

    output: dict[tuple[str, datetime], float] = {}
    for symbol, symbol_bars in by_symbol.items():
        for index in range(len(symbol_bars) - horizon):
            current = symbol_bars[index]
            future = symbol_bars[index + horizon]
            output[(symbol, current.timestamp)] = 0.0 if current.close == 0 else (future.close / current.close) - 1.0
    return output


def _top_quantile_turnovers(cross_sections: Sequence[FactorCrossSection]) -> list[float]:
    sorted_sections = sorted(cross_sections, key=lambda section: section.as_of)
    turnovers: list[float] = []
    previous: set[str] | None = None
    for section in sorted_sections:
        current = set(section.top_quantile_symbols)
        if previous is not None and current:
            turnovers.append(1.0 - (len(previous & current) / max(len(previous), len(current))))
        previous = current
    return turnovers


def _mean_or_none(values: Sequence[float]) -> float | None:
    return fmean(values) if values else None


def _information_ratio(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    dispersion = pstdev(values)
    if dispersion == 0.0:
        return None
    return fmean(values) / dispersion


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = fmean(left)
    right_mean = fmean(right)
    left_std = pstdev(left)
    right_std = pstdev(right)
    if left_std == 0.0 or right_std == 0.0:
        return None
    covariance = fmean((x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True))
    return covariance / (left_std * right_std)


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    return _pearson(_ranks(left), _ranks(right))


def _lag_one_autocorrelation(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    return _pearson(values[:-1], values[1:])


def _ranks(values: Sequence[float]) -> list[float]:
    order = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(order):
        next_index = index + 1
        while next_index < len(order) and order[next_index][1] == order[index][1]:
            next_index += 1
        average_rank = (index + 1 + next_index) / 2.0
        for position in range(index, next_index):
            ranks[order[position][0]] = average_rank
        index = next_index
    return ranks
