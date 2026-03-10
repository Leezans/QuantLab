from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from quantlab.core.models import Bar, FeatureVector, PortfolioSnapshot, Signal, TargetPosition
from quantlab.portfolio.construction import GrossExposureAllocator
from quantlab.research.alpha import AlphaFactory
from quantlab.research.evaluation import SignalEvaluation, SignalEvaluator
from quantlab.research.features import FeaturePipeline
from quantlab.risk.policies import RiskPolicyStack
from quantlab.strategy.base import SignalWeightedStrategy


@dataclass(frozen=True, slots=True)
class ResearchRunResult:
    features: tuple[FeatureVector, ...]
    signals: tuple[Signal, ...]
    targets: tuple[TargetPosition, ...]
    approved_targets: tuple[TargetPosition, ...]
    evaluations: tuple[SignalEvaluation, ...]


class ResearchWorkflow:
    def __init__(
        self,
        feature_pipeline: FeaturePipeline,
        alpha_factory: AlphaFactory,
        strategy: SignalWeightedStrategy,
        allocator: GrossExposureAllocator,
        risk_stack: RiskPolicyStack,
        evaluator: SignalEvaluator | None = None,
    ) -> None:
        self.feature_pipeline = feature_pipeline
        self.alpha_factory = alpha_factory
        self.strategy = strategy
        self.allocator = allocator
        self.risk_stack = risk_stack
        self.evaluator = evaluator or SignalEvaluator()

    def run(
        self,
        bars: Sequence[Bar],
        portfolio: PortfolioSnapshot | None = None,
    ) -> ResearchRunResult:
        if portfolio is None:
            timestamp = bars[0].timestamp if bars else datetime.now(timezone.utc)
            portfolio = PortfolioSnapshot(timestamp=timestamp, cash=1_000_000.0)

        features = self.feature_pipeline.build(bars)
        signals = self.alpha_factory.generate(features)
        evaluations = self.evaluator.summarize(signals)

        if signals:
            latest_timestamp = max(signal.as_of for signal in signals)
            latest_signals = tuple(signal for signal in signals if signal.as_of == latest_timestamp)
            targets = self.strategy.generate_targets(latest_signals, portfolio)
            approved_targets = self.risk_stack.apply(self.allocator.allocate(targets, portfolio), portfolio)
        else:
            targets = ()
            approved_targets = ()

        return ResearchRunResult(
            features=tuple(features),
            signals=tuple(signals),
            targets=tuple(targets),
            approved_targets=tuple(approved_targets),
            evaluations=tuple(evaluations),
        )

