from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from math import sqrt
from statistics import fmean, pstdev
from typing import Mapping

from quantlab.core.models import Signal


@dataclass(frozen=True, slots=True)
class SignalEvaluation:
    signal_name: str
    observations: int
    mean_score: float
    long_ratio: float
    information_coefficient: float | None = None


class SignalEvaluator:
    def summarize(
        self,
        signals: list[Signal] | tuple[Signal, ...],
        realized_returns: Mapping[tuple[str, datetime], float] | None = None,
    ) -> tuple[SignalEvaluation, ...]:
        grouped: dict[str, list[Signal]] = defaultdict(list)
        for signal in signals:
            grouped[signal.name].append(signal)

        evaluations: list[SignalEvaluation] = []
        for name, grouped_signals in sorted(grouped.items()):
            scores = [signal.value for signal in grouped_signals]
            long_ratio = sum(1 for signal in grouped_signals if signal.value > 0) / len(grouped_signals)
            information_coefficient = None
            if realized_returns:
                paired = []
                for signal in grouped_signals:
                    key = (signal.instrument.symbol, signal.as_of)
                    if key in realized_returns:
                        paired.append((signal.value, realized_returns[key]))
                information_coefficient = _pearson([left for left, _ in paired], [right for _, right in paired])

            evaluations.append(
                SignalEvaluation(
                    signal_name=name,
                    observations=len(grouped_signals),
                    mean_score=fmean(scores),
                    long_ratio=long_ratio,
                    information_coefficient=information_coefficient,
                )
            )
        return tuple(evaluations)


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    mean_left = fmean(left)
    mean_right = fmean(right)
    std_left = pstdev(left)
    std_right = pstdev(right)
    if std_left == 0 or std_right == 0:
        return None
    covariance = fmean((x - mean_left) * (y - mean_right) for x, y in zip(left, right, strict=True))
    return covariance / (std_left * std_right * sqrt(1.0))

