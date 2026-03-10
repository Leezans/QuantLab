from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quantlab.core.enums import AssetClass
from quantlab.core.models import Bar, Instrument, PortfolioSnapshot
from quantlab.orchestration.pipeline import ResearchWorkflow
from quantlab.portfolio.construction import GrossExposureAllocator
from quantlab.research.alpha import AlphaDefinition, AlphaFactory
from quantlab.research.features import FeaturePipeline, make_trailing_return_feature
from quantlab.risk.policies import MaxGrossExposurePolicy, MaxPositionWeightPolicy, RiskPolicyStack
from quantlab.strategy.base import SignalWeightedStrategy


def build_sample_bars() -> list[Bar]:
    instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    closes = [100.0, 101.0, 103.0, 102.0, 105.0, 107.0, 106.0, 109.0]
    return [
        Bar(
            timestamp=start + timedelta(days=index),
            instrument=instrument,
            open=close - 1.0,
            high=close + 1.0,
            low=close - 2.0,
            close=close,
            volume=1_000.0 + index * 5,
        )
        for index, close in enumerate(closes)
    ]


class ResearchWorkflowTestCase(unittest.TestCase):
    def test_research_workflow_generates_signals_and_targets(self) -> None:
        bars = build_sample_bars()
        workflow = ResearchWorkflow(
            feature_pipeline=FeaturePipeline([make_trailing_return_feature(lookback=3)]),
            alpha_factory=AlphaFactory(
                [AlphaDefinition(name="mom_3", score=lambda vector: vector.values["return_3"], threshold=0.0)]
            ),
            strategy=SignalWeightedStrategy(max_weight_per_instrument=0.25),
            allocator=GrossExposureAllocator(gross_target=1.0),
            risk_stack=RiskPolicyStack([MaxPositionWeightPolicy(0.2), MaxGrossExposurePolicy(1.0)]),
        )

        result = workflow.run(bars, PortfolioSnapshot(timestamp=bars[0].timestamp, cash=1_000_000.0))

        self.assertEqual(len(result.features), 6)
        self.assertEqual(len(result.signals), 6)
        self.assertEqual(len(result.evaluations), 1)
        self.assertEqual(len(result.approved_targets), 1)
        self.assertLessEqual(abs(result.approved_targets[0].target_weight), 0.2)


if __name__ == "__main__":
    unittest.main()

