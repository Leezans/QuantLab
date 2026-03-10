from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from quantlab.core.models import Bar, FeatureVector
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quantlab.orchestration.formula_pipeline import FormulaicFactorWorkflow, FormulaicResearchResult


@dataclass(frozen=True, slots=True)
class MiningSweepEntry:
    label: str
    workflow_builder: Callable[[], "FormulaicFactorWorkflow"]


@dataclass(frozen=True, slots=True)
class MiningSweepRun:
    label: str
    result: "FormulaicResearchResult"


@dataclass(frozen=True, slots=True)
class MiningSweepComparison:
    runs: tuple[MiningSweepRun, ...]

    def rank_by_best_fitness(self) -> tuple[MiningSweepRun, ...]:
        return tuple(
            sorted(
                self.runs,
                key=lambda run: max((candidate.fitness for candidate in run.result.factor_search.candidates), default=float("-inf")),
                reverse=True,
            )
        )

    def rank_by_composite_sharpe(self) -> tuple[MiningSweepRun, ...]:
        def composite_sharpe(run: MiningSweepRun) -> float:
            composite = next(
                (backtest for backtest in run.result.backtests if backtest.factor_name == "composite.ic_weighted"),
                None,
            )
            return composite.report.sharpe if composite and composite.report.sharpe is not None else float("-inf")

        return tuple(sorted(self.runs, key=composite_sharpe, reverse=True))


class FormulaicMiningSweepRunner:
    def run(
        self,
        entries: Sequence[MiningSweepEntry],
        *,
        features: Sequence[FeatureVector],
        bars: Sequence[Bar],
        version_prefix: str,
        metadata: dict[str, str] | None = None,
    ) -> MiningSweepComparison:
        runs: list[MiningSweepRun] = []
        for index, entry in enumerate(entries):
            workflow = entry.workflow_builder()
            result = workflow.run(
                features,
                bars,
                version=f"{version_prefix}-{index:02d}",
                metadata={**(metadata or {}), "sweep_label": entry.label},
            )
            runs.append(MiningSweepRun(label=entry.label, result=result))
        return MiningSweepComparison(runs=tuple(runs))
