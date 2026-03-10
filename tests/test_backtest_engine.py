from __future__ import annotations

import unittest

from quantlab.backtest.costs import FixedBpsTransactionCostModel
from quantlab.backtest.engine import SimpleBacktestEngine
from quantlab.core.models import PortfolioSnapshot
from quantlab.execution.algorithms import ImmediateExecutionAlgorithm
from quantlab.orchestration.pipeline import ResearchWorkflow
from quantlab.portfolio.construction import GrossExposureAllocator
from quantlab.research.alpha import AlphaDefinition, AlphaFactory
from quantlab.research.features import FeaturePipeline, make_trailing_return_feature
from quantlab.risk.policies import MaxGrossExposurePolicy, MaxPositionWeightPolicy, RiskPolicyStack
from quantlab.strategy.base import SignalWeightedStrategy
from tests.test_research_pipeline import build_sample_bars


class BacktestEngineTestCase(unittest.TestCase):
    def test_backtest_engine_produces_metrics(self) -> None:
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
        research_run = workflow.run(bars, PortfolioSnapshot(timestamp=bars[0].timestamp, cash=1_000_000.0))
        engine = SimpleBacktestEngine(
            strategy=workflow.strategy,
            allocator=workflow.allocator,
            risk_policy=workflow.risk_stack,
            execution_algorithm=ImmediateExecutionAlgorithm(),
            cost_model=FixedBpsTransactionCostModel(bps=1.0),
            initial_cash=1_000_000.0,
        )

        result = engine.run(bars, research_run.signals)

        self.assertEqual(len(result.equity_curve), len(bars))
        self.assertGreater(len(result.orders), 0)
        self.assertGreater(result.report.turnover, 0.0)
        self.assertGreaterEqual(result.report.max_drawdown, 0.0)


if __name__ == "__main__":
    unittest.main()

