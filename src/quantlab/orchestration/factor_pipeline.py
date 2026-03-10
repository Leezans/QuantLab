from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from quantlab.core.models import Bar, FeatureVector
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
from quantlab.research.factors import (
    CandidateFactorGenerator,
    FactorDefinition,
    FactorExposure,
    FactorMiner,
    collect_feature_names,
)
from quantlab.research.features import FeaturePipeline


@dataclass(frozen=True, slots=True)
class FactorResearchResult:
    features: tuple[FeatureVector, ...]
    factor_definitions: tuple[FactorDefinition, ...]
    factor_exposures: tuple[FactorExposure, ...]
    cross_sections: tuple[FactorCrossSection, ...]
    summaries: tuple[FactorSummary, ...]
    decay: tuple[FactorDecayPoint, ...]
    selected_summaries: tuple[FactorSummary, ...]
    selected_exposures: tuple[FactorExposure, ...]
    composite_exposures: tuple[FactorExposure, ...]
    composite_cross_sections: tuple[FactorCrossSection, ...]
    composite_summaries: tuple[FactorSummary, ...]
    composite_decay: tuple[FactorDecayPoint, ...]
    combination_weights: tuple[FactorWeight, ...]
    backtests: tuple[FactorBacktestResult, ...]
    forward_returns: dict[tuple[str, datetime], float]
    artifacts: tuple[FactorArtifactRef, ...]


class FactorResearchWorkflow:
    def __init__(
        self,
        feature_pipeline: FeaturePipeline,
        factor_miner: FactorMiner | None = None,
        candidate_generator: CandidateFactorGenerator | None = None,
        evaluator: FactorEvaluator | None = None,
        selector: ThresholdFactorSelector | None = None,
        combiner: ICWeightFactorCombiner | EqualWeightFactorCombiner | None = None,
        backtester: QuantileLongShortBacktester | None = None,
        factor_store: LocalFactorStore | None = None,
        artifact_base_path: Path | None = None,
        artifact_name: str = "factor_research",
        forward_horizon: int = 1,
        decay_horizons: Sequence[int] = (1, 3, 5),
        combine_top_n: int | None = 5,
    ) -> None:
        self._feature_pipeline = feature_pipeline
        self._factor_miner = factor_miner
        self._candidate_generator = candidate_generator or CandidateFactorGenerator()
        self._evaluator = evaluator or FactorEvaluator()
        self._selector = selector
        self._combiner = combiner
        self._backtester = backtester
        self._factor_store = factor_store
        self._artifact_base_path = artifact_base_path
        self._artifact_name = artifact_name
        self._forward_horizon = forward_horizon
        self._decay_horizons = tuple(decay_horizons)
        self._combine_top_n = combine_top_n

    def run(
        self,
        bars: Sequence[Bar],
        *,
        version: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> FactorResearchResult:
        features = tuple(self._feature_pipeline.build(bars))
        factor_miner = self._factor_miner or FactorMiner(
            self._candidate_generator.generate(collect_feature_names(features))
        )
        factor_exposures = tuple(factor_miner.mine(features))
        forward_returns = build_forward_returns(bars, horizon=self._forward_horizon)
        cross_sections = self._evaluator.cross_sections(factor_exposures, forward_returns)
        summaries = self._evaluator.summarize(cross_sections)
        decay = self._evaluator.decay(factor_exposures, bars, self._decay_horizons)

        selected_summaries = self._selector.select(summaries) if self._selector else summaries
        selected_names = {summary.factor_name for summary in selected_summaries}
        selected_exposures = tuple(exposure for exposure in factor_exposures if exposure.factor_name in selected_names)

        composite_exposures: tuple[FactorExposure, ...] = ()
        combination_weights: tuple[FactorWeight, ...] = ()
        composite_cross_sections: tuple[FactorCrossSection, ...] = ()
        composite_summaries: tuple[FactorSummary, ...] = ()
        composite_decay: tuple[FactorDecayPoint, ...] = ()
        if self._combiner and selected_summaries:
            composite_exposures, combination_weights = self._combiner.combine(
                selected_exposures,
                selected_summaries,
                top_n=self._combine_top_n,
            )
            if composite_exposures:
                composite_cross_sections = self._evaluator.cross_sections(composite_exposures, forward_returns)
                composite_summaries = self._evaluator.summarize(composite_cross_sections)
                composite_decay = self._evaluator.decay(composite_exposures, bars, self._decay_horizons)

        backtests = self._build_backtests(cross_sections, composite_cross_sections, selected_names)
        artifacts = self._persist_artifacts(
            version=version,
            metadata=metadata or {},
            selected_names=selected_names,
            exposures=tuple((*factor_exposures, *composite_exposures)),
            summaries=tuple((*summaries, *composite_summaries)),
            cross_sections=tuple((*cross_sections, *composite_cross_sections)),
            decay=tuple((*decay, *composite_decay)),
            weights=combination_weights,
            backtests=backtests,
        )

        return FactorResearchResult(
            features=features,
            factor_definitions=factor_miner.definitions,
            factor_exposures=factor_exposures,
            cross_sections=cross_sections,
            summaries=summaries,
            decay=decay,
            selected_summaries=selected_summaries,
            selected_exposures=selected_exposures,
            composite_exposures=composite_exposures,
            composite_cross_sections=composite_cross_sections,
            composite_summaries=composite_summaries,
            composite_decay=composite_decay,
            combination_weights=combination_weights,
            backtests=backtests,
            forward_returns=forward_returns,
            artifacts=artifacts,
        )

    def _build_backtests(
        self,
        cross_sections: Sequence[FactorCrossSection],
        composite_cross_sections: Sequence[FactorCrossSection],
        selected_names: set[str],
    ) -> tuple[FactorBacktestResult, ...]:
        if self._backtester is None:
            return ()
        scoped_cross_sections = tuple(
            section
            for section in cross_sections
            if section.factor_name in selected_names
        )
        return self._backtester.run(tuple((*scoped_cross_sections, *composite_cross_sections)))

    def _persist_artifacts(
        self,
        *,
        version: str | None,
        metadata: Mapping[str, str],
        selected_names: set[str],
        exposures: Sequence[FactorExposure],
        summaries: Sequence[FactorSummary],
        cross_sections: Sequence[FactorCrossSection],
        decay: Sequence[FactorDecayPoint],
        weights: Sequence[FactorWeight],
        backtests: Sequence[FactorBacktestResult],
    ) -> tuple[FactorArtifactRef, ...]:
        if self._factor_store is None or self._artifact_base_path is None:
            return ()

        artifact_version = version or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        artifact_metadata = {
            **{key: str(value) for key, value in metadata.items()},
            "forward_horizon": str(self._forward_horizon),
            "selected_factor_count": str(len(selected_names)),
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
