from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from quantlab.core.models import Bar, FeatureVector
from quantlab.research.factors import (
    CandidateFactorGenerator,
    FactorDefinition,
    FactorExposure,
    FactorMiner,
    collect_feature_names,
)
from quantlab.research.regime import (
    RegimeConditionedDecayPoint,
    RegimeConditionedFactorSummary,
    RegimeInferenceResult,
    RegimeModelArtifact,
    RegimeObservationFrame,
    RegimeProfile,
    RegimeSignal,
)
from quantlab.research.regime_analysis import RegimeAnalyzer, RegimeConditionedFactorEvaluator
from quantlab.research.regime_features import CrossSectionalRegimeObservationBuilder


@dataclass(frozen=True, slots=True)
class RegimeResearchResult:
    observation_frame: RegimeObservationFrame
    artifact: RegimeModelArtifact
    inference: RegimeInferenceResult
    durations: tuple
    profiles: tuple[RegimeProfile, ...]
    factor_definitions: tuple[FactorDefinition, ...]
    factor_exposures: tuple[FactorExposure, ...]
    conditioned_summaries: tuple[RegimeConditionedFactorSummary, ...]
    conditioned_decay: tuple[RegimeConditionedDecayPoint, ...]
    current_signal: RegimeSignal | None


class RegimeResearchWorkflow:
    def __init__(
        self,
        observation_builder: CrossSectionalRegimeObservationBuilder,
        model,
        analyzer: RegimeAnalyzer | None = None,
        conditioned_evaluator: RegimeConditionedFactorEvaluator | None = None,
        factor_miner: FactorMiner | None = None,
        candidate_generator: CandidateFactorGenerator | None = None,
    ) -> None:
        self._observation_builder = observation_builder
        self._model = model
        self._analyzer = analyzer or RegimeAnalyzer()
        self._conditioned_evaluator = conditioned_evaluator or RegimeConditionedFactorEvaluator()
        self._factor_miner = factor_miner
        self._candidate_generator = candidate_generator or CandidateFactorGenerator()

    def run(
        self,
        features: Sequence[FeatureVector],
        bars: Sequence[Bar],
        *,
        regime_scope: str = "market",
        factor_definitions: Sequence[FactorDefinition] | None = None,
        factor_feature_names: Sequence[str] | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> RegimeResearchResult:
        del metadata
        frame = self._observation_builder.build_market_frame(features, scope=regime_scope)
        artifact = self._model.fit(frame)
        inference = self._model.infer(frame, artifact)
        durations = self._analyzer.duration_summaries(inference)
        profiles = self._analyzer.profiles(frame, inference, bars)

        definitions = tuple(factor_definitions or self._build_factor_definitions(features, factor_feature_names))
        miner = self._factor_miner or FactorMiner(definitions)
        exposures = tuple(miner.mine(features)) if definitions else ()
        conditioned_summaries, conditioned_decay = self._conditioned_evaluator.evaluate(exposures, bars, inference)

        latest = inference.latest
        current_signal = None
        if latest is not None:
            current_signal = RegimeSignal(
                as_of=latest.as_of,
                scope=latest.scope,
                state_id=latest.state_id,
                confidence=max(latest.probabilities.values(), default=0.0),
                transition_probabilities=inference.transition_matrix.row(latest.state_id),
            )
        return RegimeResearchResult(
            observation_frame=frame,
            artifact=artifact,
            inference=inference,
            durations=durations,
            profiles=profiles,
            factor_definitions=definitions,
            factor_exposures=exposures,
            conditioned_summaries=conditioned_summaries,
            conditioned_decay=conditioned_decay,
            current_signal=current_signal,
        )

    def _build_factor_definitions(
        self,
        features: Sequence[FeatureVector],
        factor_feature_names: Sequence[str] | None,
    ) -> tuple[FactorDefinition, ...]:
        available = collect_feature_names(features)
        if factor_feature_names:
            available = tuple(name for name in available if name in set(factor_feature_names))
        return self._candidate_generator.generate(available)
