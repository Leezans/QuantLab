from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from quantlab.research.factor_evaluation import FactorSummary


@dataclass(frozen=True, slots=True)
class FactorSelectionPolicy:
    min_mean_ic: float = 0.0
    min_hit_rate: float = 0.5
    min_cross_sections: int = 3
    min_mean_quantile_spread: float = 0.0


class ThresholdFactorSelector:
    def __init__(self, policy: FactorSelectionPolicy | None = None) -> None:
        self._policy = policy or FactorSelectionPolicy()

    @property
    def policy(self) -> FactorSelectionPolicy:
        return self._policy

    def select(self, summaries: Sequence[FactorSummary]) -> tuple[FactorSummary, ...]:
        selected = [
            summary
            for summary in summaries
            if summary.mean_ic is not None
            and summary.mean_ic >= self._policy.min_mean_ic
            and summary.cross_sections >= self._policy.min_cross_sections
            and (summary.hit_rate or 0.0) >= self._policy.min_hit_rate
            and (summary.mean_quantile_spread or 0.0) >= self._policy.min_mean_quantile_spread
        ]
        return tuple(sorted(selected, key=lambda summary: (summary.mean_ic or 0.0, summary.mean_quantile_spread or 0.0), reverse=True))
