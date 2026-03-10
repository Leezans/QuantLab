from __future__ import annotations

from datetime import datetime, timedelta, timezone

from quantlab.backtest.costs import FixedBpsTransactionCostModel
from quantlab.backtest.engine import SimpleBacktestEngine
from quantlab.core.enums import AssetClass
from quantlab.core.models import Instrument, PortfolioSnapshot
from quantlab.execution.algorithms import ImmediateExecutionAlgorithm
from quantlab.orchestration.pipeline import ResearchWorkflow
from quantlab.portfolio.construction import GrossExposureAllocator
from quantlab.research.alpha import AlphaDefinition, AlphaFactory
from quantlab.research.features import FeaturePipeline, make_trailing_return_feature
from quantlab.risk.policies import MaxGrossExposurePolicy, MaxPositionWeightPolicy, RiskPolicyStack
from quantlab.strategy.base import SignalWeightedStrategy


def build_sample_bars() -> list:
    instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    closes = [100.0, 101.0, 103.0, 102.0, 105.0, 107.0, 106.0, 109.0]
    bars = []
    for offset, close in enumerate(closes):
        timestamp = start + timedelta(days=offset)
        bars.append(
            {
                "timestamp": timestamp,
                "instrument": instrument,
                "open": close - 1.0,
                "high": close + 1.0,
                "low": close - 2.0,
                "close": close,
                "volume": 1000.0 + offset * 10,
            }
        )
    from quantlab.core.models import Bar

    return [Bar(**bar) for bar in bars]


def main() -> None:
    bars = build_sample_bars()
    feature_pipeline = FeaturePipeline([make_trailing_return_feature(lookback=3)])
    alpha_factory = AlphaFactory(
        [AlphaDefinition(name="mom_3", score=lambda vector: vector.values["return_3"], threshold=0.0)]
    )
    workflow = ResearchWorkflow(
        feature_pipeline=feature_pipeline,
        alpha_factory=alpha_factory,
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
    print("signals:", len(research_run.signals))
    print("approved targets:", len(research_run.approved_targets))
    print("pnl:", round(result.report.pnl, 2))
    print("sharpe:", round(result.report.sharpe, 4))


if __name__ == "__main__":
    main()

