from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quantlab.core.enums import AssetClass
from quantlab.core.models import Bar, FeatureVector, Instrument
from quantlab.orchestration.regime_pipeline import RegimeResearchWorkflow
from quantlab.research.factor_evaluation import FactorEvaluator
from quantlab.research.factors import CandidateFactorGenerator, FactorNormalization
from quantlab.research.regime_analysis import RegimeAnalyzer, RegimeConditionedFactorEvaluator
from quantlab.research.regime_features import CrossSectionalRegimeObservationBuilder
from quantlab.research.regime_models import GaussianHMMConfig, GaussianHMMRegimeModel


def build_regime_bars_and_features() -> tuple[list[Bar], list[FeatureVector]]:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    symbols = ("BTCUSDT", "ETHUSDT")
    instruments = {
        symbol: Instrument(symbol=symbol, venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        for symbol in symbols
    }
    bars: list[Bar] = []
    features: list[FeatureVector] = []
    btc_prices = [100.0, 99.6, 99.4, 99.2, 99.1, 99.0, 100.0, 101.5, 103.0, 104.8, 106.0, 107.5]
    eth_prices = [80.0, 79.7, 79.5, 79.4, 79.2, 79.0, 79.8, 80.9, 82.1, 83.0, 84.0, 85.2]
    price_map = {"BTCUSDT": btc_prices, "ETHUSDT": eth_prices}
    for symbol in symbols:
        prices = price_map[symbol]
        for index, close in enumerate(prices):
            timestamp = start + timedelta(minutes=index)
            bars.append(
                Bar(
                    timestamp=timestamp,
                    instrument=instruments[symbol],
                    open=close - 0.2,
                    high=close + 0.4,
                    low=close - 0.4,
                    close=close,
                    volume=1_000.0 + (50.0 * index),
                )
            )
            low_regime = index < 6
            symbol_shift = 0.02 if symbol == "BTCUSDT" else -0.01
            features.append(
                FeatureVector(
                    as_of=timestamp,
                    instrument=instruments[symbol],
                    values={
                        "return_3": (-0.03 if low_regime else 0.05) + symbol_shift,
                        "return_5": (-0.05 if low_regime else 0.08) + symbol_shift,
                        "realized_volatility_5": 0.010 if low_regime else 0.045,
                        "signed_quantity_imbalance": -0.55 if low_regime else 0.65,
                        "vpin_proxy": 0.20 if low_regime else 0.82,
                        "volume_profile_tilt": -0.35 if low_regime else 0.45,
                        "bar_volume_ratio_5": 0.85 if low_regime else 1.30,
                    },
                )
            )
    return bars, features


class RegimeObservationBuilderTestCase(unittest.TestCase):
    def test_builds_cross_sectional_market_frame(self) -> None:
        _, features = build_regime_bars_and_features()
        frame = CrossSectionalRegimeObservationBuilder(
            feature_names=("return_3", "vpin_proxy"),
            aggregations=("mean", "stdev", "breadth"),
        ).build_market_frame(features, scope="crypto_market")

        self.assertEqual(frame.scope, "crypto_market")
        self.assertEqual(len(frame.observations), 12)
        self.assertIn("return_3__mean", frame.feature_names)
        self.assertIn("vpin_proxy__breadth", frame.feature_names)


class GaussianHMMRegimeModelTestCase(unittest.TestCase):
    def test_fits_and_infers_two_regimes(self) -> None:
        _, features = build_regime_bars_and_features()
        frame = CrossSectionalRegimeObservationBuilder(
            feature_names=("return_3", "realized_volatility_5", "vpin_proxy"),
            aggregations=("mean", "stdev"),
        ).build_market_frame(features)
        model = GaussianHMMRegimeModel(GaussianHMMConfig(n_states=2, max_iterations=12, random_seed=11))

        artifact = model.fit(frame)
        inference = model.infer(frame, artifact)

        self.assertEqual(artifact.state_count, 2)
        self.assertEqual(len(inference.states), len(frame.observations))
        self.assertGreaterEqual(len({estimate.state_id for estimate in inference.states}), 2)
        self.assertIsNotNone(inference.latest)


class RegimeWorkflowTestCase(unittest.TestCase):
    def test_regime_workflow_produces_conditioned_factor_summaries(self) -> None:
        bars, features = build_regime_bars_and_features()
        workflow = RegimeResearchWorkflow(
            observation_builder=CrossSectionalRegimeObservationBuilder(
                feature_names=("return_3", "realized_volatility_5", "signed_quantity_imbalance", "vpin_proxy", "volume_profile_tilt"),
                aggregations=("mean", "stdev", "breadth"),
            ),
            model=GaussianHMMRegimeModel(GaussianHMMConfig(n_states=2, max_iterations=12, random_seed=13)),
            analyzer=RegimeAnalyzer(),
            conditioned_evaluator=RegimeConditionedFactorEvaluator(FactorEvaluator(min_observations=2)),
            candidate_generator=CandidateFactorGenerator(
                normalizations=(FactorNormalization.RAW, FactorNormalization.ZSCORE),
                include_inverse=False,
            ),
        )

        result = workflow.run(
            features,
            bars,
            regime_scope="workflow_market",
            factor_feature_names=("return_3", "vpin_proxy", "volume_profile_tilt"),
        )

        self.assertTrue(result.durations)
        self.assertTrue(result.profiles)
        self.assertTrue(result.conditioned_summaries)
        self.assertIsNotNone(result.current_signal)


if __name__ == "__main__":
    unittest.main()
