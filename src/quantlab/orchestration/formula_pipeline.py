from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from quantlab.core.models import Bar, FeatureVector
from quantlab.experiments.tracker import ExperimentRun, LocalExperimentTracker
from quantlab.research.factor_backtest import FactorBacktestResult, QuantileLongShortBacktester
from quantlab.research.factor_combination import (
    EqualWeightFactorCombiner,
    FactorWeight,
    ICWeightFactorCombiner,
)
from quantlab.research.factor_evaluation import (
    FactorCrossSection,
    FactorDecayPoint,
    FactorEvaluator,
    FactorSummary,
    build_forward_returns,
)
from quantlab.research.factor_selection import ThresholdFactorSelector
from quantlab.research.factor_storage import FactorArtifactRef, LocalFactorStore
from quantlab.research.formulaic_search import (
    FormulaCandidate,
    FormulaSearchGeneration,
    FormulaSearchResult,
)


@dataclass(frozen=True, slots=True)
class FormulaicResearchResult:
    features: tuple[FeatureVector, ...]
    factor_search: FormulaSearchResult
    cross_sections: tuple[FactorCrossSection, ...]
    summaries: tuple[FactorSummary, ...]
    decay: tuple[FactorDecayPoint, ...]
    selected_summaries: tuple[FactorSummary, ...]
    composite_cross_sections: tuple[FactorCrossSection, ...]
    composite_summaries: tuple[FactorSummary, ...]
    composite_decay: tuple[FactorDecayPoint, ...]
    combination_weights: tuple[FactorWeight, ...]
    backtests: tuple[FactorBacktestResult, ...]
    forward_returns: dict[tuple[str, datetime], float]
    artifacts: tuple[FactorArtifactRef, ...]
    experiment_path: Path | None


class FormulaicFactorWorkflow:
    def __init__(
        self,
        miner,
        evaluator: FactorEvaluator | None = None,
        selector: ThresholdFactorSelector | None = None,
        combiner: ICWeightFactorCombiner | EqualWeightFactorCombiner | None = None,
        backtester: QuantileLongShortBacktester | None = None,
        factor_store: LocalFactorStore | None = None,
        artifact_base_path: Path | None = None,
        artifact_name: str = "formulaic_factor_research",
        tracker: LocalExperimentTracker | None = None,
        decay_horizons: Sequence[int] = (1, 3, 5),
    ) -> None:
        self._miner = miner
        self._evaluator = evaluator or FactorEvaluator()
        self._selector = selector
        self._combiner = combiner
        self._backtester = backtester
        self._factor_store = factor_store
        self._artifact_base_path = artifact_base_path
        self._artifact_name = artifact_name
        self._tracker = tracker
        self._decay_horizons = tuple(decay_horizons)

    def run(
        self,
        features: Sequence[FeatureVector],
        bars: Sequence[Bar],
        *,
        version: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> FormulaicResearchResult:
        search_result = self._miner.mine(features, bars)
        forward_returns = build_forward_returns(bars, horizon=1)
        cross_sections = self._evaluator.cross_sections(search_result.factor_exposures, forward_returns)
        summaries = self._evaluator.summarize(cross_sections)
        decay = self._evaluator.decay(search_result.factor_exposures, bars, self._decay_horizons)

        selected_summaries = self._selector.select(summaries) if self._selector else summaries
        selected_names = {summary.factor_name for summary in selected_summaries}
        selected_exposures = tuple(
            exposure
            for exposure in search_result.factor_exposures
            if exposure.factor_name in selected_names
        )

        composite_cross_sections: tuple[FactorCrossSection, ...] = ()
        composite_summaries: tuple[FactorSummary, ...] = ()
        composite_decay: tuple[FactorDecayPoint, ...] = ()
        composite_exposures = ()
        combination_weights: tuple[FactorWeight, ...] = ()
        if self._combiner and selected_summaries:
            composite_exposures, combination_weights = self._combiner.combine(
                selected_exposures,
                selected_summaries,
            )
            if composite_exposures:
                composite_cross_sections = self._evaluator.cross_sections(composite_exposures, forward_returns)
                composite_summaries = self._evaluator.summarize(composite_cross_sections)
                composite_decay = self._evaluator.decay(composite_exposures, bars, self._decay_horizons)

        scoped_cross_sections = tuple(
            cross_section
            for cross_section in cross_sections
            if cross_section.factor_name in selected_names
        )
        backtests = self._backtester.run(tuple((*scoped_cross_sections, *composite_cross_sections))) if self._backtester else ()
        artifacts = self._persist_artifacts(
            version=version,
            metadata=metadata or {},
            search_result=search_result,
            exposures=tuple((*search_result.factor_exposures, *composite_exposures)),
            summaries=tuple((*summaries, *composite_summaries)),
            cross_sections=tuple((*cross_sections, *composite_cross_sections)),
            decay=tuple((*decay, *composite_decay)),
            weights=combination_weights,
            backtests=backtests,
        )
        experiment_path = self._log_experiment(
            version=version,
            metadata=metadata or {},
            candidates=search_result.candidates,
            selected_summaries=selected_summaries,
            backtests=backtests,
        )
        return FormulaicResearchResult(
            features=tuple(features),
            factor_search=search_result,
            cross_sections=cross_sections,
            summaries=summaries,
            decay=decay,
            selected_summaries=selected_summaries,
            composite_cross_sections=composite_cross_sections,
            composite_summaries=composite_summaries,
            composite_decay=composite_decay,
            combination_weights=combination_weights,
            backtests=backtests,
            forward_returns=forward_returns,
            artifacts=artifacts,
            experiment_path=experiment_path,
        )

    def _persist_artifacts(
        self,
        *,
        version: str | None,
        metadata: Mapping[str, str],
        search_result: FormulaSearchResult,
        exposures,
        summaries,
        cross_sections,
        decay,
        weights,
        backtests,
    ) -> tuple[FactorArtifactRef, ...]:
        if self._factor_store is None or self._artifact_base_path is None:
            return ()
        artifact_version = version or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        artifact_metadata = {
            **{key: str(value) for key, value in metadata.items()},
            "miner": search_result.miner,
            "candidate_count": str(len(search_result.candidates)),
        }
        artifacts = [
            self._factor_store.write_exposures(
                self._artifact_name,
                artifact_version,
                exposures,
                self._artifact_base_path,
                artifact_metadata,
            ),
        ]
        artifacts.extend(
            self._factor_store.write_summaries(
                self._artifact_name,
                artifact_version,
                summaries,
                cross_sections,
                self._artifact_base_path,
                artifact_metadata,
            )
        )
        if decay:
            artifacts.append(
                self._factor_store.write_decay(
                    self._artifact_name,
                    artifact_version,
                    decay,
                    self._artifact_base_path,
                    artifact_metadata,
                )
            )
        if weights:
            artifacts.append(
                self._factor_store.write_weights(
                    self._artifact_name,
                    artifact_version,
                    weights,
                    self._artifact_base_path,
                    artifact_metadata,
                )
            )
        if backtests:
            artifacts.extend(
                self._factor_store.write_backtests(
                    self._artifact_name,
                    artifact_version,
                    [result.report for result in backtests],
                    [point for result in backtests for point in result.series],
                    self._artifact_base_path,
                    artifact_metadata,
                )
            )
        return tuple(artifacts)

    def _log_experiment(
        self,
        *,
        version: str | None,
        metadata: Mapping[str, str],
        candidates: Sequence[FormulaCandidate],
        selected_summaries: Sequence[FactorSummary],
        backtests: Sequence[FactorBacktestResult],
    ) -> Path | None:
        if self._tracker is None:
            return None
        best_candidate = max(candidates, key=lambda item: item.fitness, default=None)
        composite = next((result for result in backtests if result.factor_name == "composite.ic_weighted"), None)
        run = ExperimentRun(
            name=self._artifact_name,
            started_at=datetime.now(timezone.utc),
            parameters={
                **{key: str(value) for key, value in metadata.items()},
                "version": version or "latest",
            },
            metrics={
                "best_fitness": best_candidate.fitness if best_candidate else 0.0,
                "best_mean_ic": best_candidate.mean_ic or 0.0 if best_candidate else 0.0,
                "selected_factor_count": float(len(selected_summaries)),
                "composite_sharpe": composite.report.sharpe if composite and composite.report.sharpe is not None else 0.0,
            },
            notes=best_candidate.formula if best_candidate else "",
        )
        return self._tracker.log_run(run)
