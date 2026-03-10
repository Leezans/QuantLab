from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from math import sqrt
from statistics import fmean, pstdev

from quantlab.research.factor_evaluation import FactorCrossSection


@dataclass(frozen=True, slots=True)
class FactorReturnPoint:
    factor_name: str
    as_of: datetime
    long_return: float
    short_return: float
    long_short_return: float


@dataclass(frozen=True, slots=True)
class FactorBacktestReport:
    factor_name: str
    periods: int
    cumulative_return: float
    annualized_return: float | None
    volatility: float | None
    sharpe: float | None
    max_drawdown: float
    hit_rate: float | None


@dataclass(frozen=True, slots=True)
class FactorBacktestResult:
    factor_name: str
    series: tuple[FactorReturnPoint, ...]
    report: FactorBacktestReport


class QuantileLongShortBacktester:
    def __init__(self, periods_per_year: int = 365) -> None:
        self._periods_per_year = periods_per_year

    def run(self, cross_sections: Sequence[FactorCrossSection]) -> tuple[FactorBacktestResult, ...]:
        grouped: dict[str, list[FactorCrossSection]] = defaultdict(list)
        for cross_section in cross_sections:
            grouped[cross_section.factor_name].append(cross_section)

        results: list[FactorBacktestResult] = []
        for factor_name, dated_sections in sorted(grouped.items()):
            series = tuple(
                FactorReturnPoint(
                    factor_name=factor_name,
                    as_of=section.as_of,
                    long_return=section.top_quantile_return or 0.0,
                    short_return=-(section.bottom_quantile_return or 0.0),
                    long_short_return=section.quantile_spread or 0.0,
                )
                for section in sorted(dated_sections, key=lambda value: value.as_of)
            )
            returns = [point.long_short_return for point in series]
            cumulative_path = _cumulative_path(returns)
            periods = len(returns)
            volatility = pstdev(returns) * sqrt(self._periods_per_year) if len(returns) > 1 else None
            average_return = fmean(returns) if returns else 0.0
            annualized_return = ((1.0 + average_return) ** self._periods_per_year) - 1.0 if returns else None
            sharpe = None
            if volatility and volatility > 0.0:
                sharpe = (average_return * self._periods_per_year) / volatility
            results.append(
                FactorBacktestResult(
                    factor_name=factor_name,
                    series=series,
                    report=FactorBacktestReport(
                        factor_name=factor_name,
                        periods=periods,
                        cumulative_return=(cumulative_path[-1] - 1.0) if cumulative_path else 0.0,
                        annualized_return=annualized_return,
                        volatility=volatility,
                        sharpe=sharpe,
                        max_drawdown=_max_drawdown(cumulative_path),
                        hit_rate=(sum(1 for value in returns if value > 0) / len(returns)) if returns else None,
                    ),
                )
            )
        return tuple(results)


def _cumulative_path(returns: Sequence[float]) -> list[float]:
    path: list[float] = []
    capital = 1.0
    for value in returns:
        capital *= 1.0 + value
        path.append(capital)
    return path


def _max_drawdown(path: Sequence[float]) -> float:
    peak = 1.0
    max_drawdown = 0.0
    for value in path:
        peak = max(peak, value)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - value) / peak)
    return max_drawdown
