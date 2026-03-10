from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from quantlab.core.enums import AssetClass, Side
from quantlab.core.models import Bar, FeatureVector, Instrument, OrderBookLevel, OrderBookSnapshot, Trade
from quantlab.data import DataCatalog
from quantlab.experiments.tracker import LocalExperimentTracker
from quantlab.orchestration.formula_pipeline import FormulaicFactorWorkflow
from quantlab.research.batch import FormulaicMiningSweepRunner, MiningSweepEntry
from quantlab.research.curation import BinanceCuratedFeatureBuilder, CuratedFeatureDatasetService, ParquetFeatureFrameStore
from quantlab.research.factor_backtest import QuantileLongShortBacktester
from quantlab.research.factor_combination import ICWeightFactorCombiner
from quantlab.research.factor_selection import FactorSelectionPolicy, ThresholdFactorSelector
from quantlab.research.factor_storage import FactorCatalog, LocalFactorStore
from quantlab.research.gp_mining import GeneticProgrammingConfig, GeneticProgrammingFactorMiner
from quantlab.research.rl_mining import PolicyGradientConfig, PolicyGradientFactorMiner


def has_optional_dependency(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def build_microstructure_bars() -> list[Bar]:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    trajectories = {
        "BTCUSDT": [100.0, 101.0, 102.5, 104.0, 105.5, 107.0, 108.5, 110.0, 111.5, 113.0, 114.5, 116.0],
        "ETHUSDT": [80.0, 80.6, 81.2, 81.8, 82.4, 83.0, 83.6, 84.2, 84.8, 85.4, 86.0, 86.6],
        "SOLUSDT": [50.0, 49.8, 49.5, 49.0, 48.5, 48.0, 47.5, 47.0, 46.5, 46.0, 45.5, 45.0],
    }
    bars: list[Bar] = []
    for symbol, closes in trajectories.items():
        instrument = Instrument(symbol=symbol, venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        for offset, close in enumerate(closes):
            timestamp = start + timedelta(minutes=offset)
            bars.append(
                Bar(
                    timestamp=timestamp,
                    instrument=instrument,
                    open=close - 0.3,
                    high=close + 0.6,
                    low=close - 0.6,
                    close=close,
                    volume=1_000 + offset * 20,
                )
            )
    return bars


def build_microstructure_trades(bars: list[Bar]) -> list[Trade]:
    trades: list[Trade] = []
    for bar in bars:
        for offset in range(5):
            direction = 1 if bar.instrument.symbol != "SOLUSDT" or offset % 2 == 0 else -1
            side = "buy" if direction > 0 else "sell"
            trades.append(
                Trade(
                    timestamp=bar.timestamp + timedelta(seconds=offset * 5),
                    instrument=bar.instrument,
                    trade_id=f"{bar.instrument.symbol}-{bar.timestamp.isoformat()}-{offset}",
                    price=bar.open + (offset + 1) * 0.05 * direction,
                    quantity=0.3 + offset * 0.05,
                    side=Side.BUY if side == "buy" else Side.SELL,
                )
            )
    return trades


def build_microstructure_books(bars: list[Bar]) -> list[OrderBookSnapshot]:
    snapshots: list[OrderBookSnapshot] = []
    for bar in bars:
        snapshots.append(
            OrderBookSnapshot(
                timestamp=bar.timestamp + timedelta(seconds=50),
                instrument=bar.instrument,
                sequence_id=f"{bar.instrument.symbol}-{bar.timestamp.isoformat()}",
                bids=(
                    OrderBookLevel(side=Side.BUY, level=1, price=bar.close - 0.03, quantity=1.5),
                    OrderBookLevel(side=Side.BUY, level=2, price=bar.close - 0.05, quantity=2.0),
                ),
                asks=(
                    OrderBookLevel(side=Side.SELL, level=1, price=bar.close + 0.03, quantity=1.2),
                    OrderBookLevel(side=Side.SELL, level=2, price=bar.close + 0.05, quantity=1.7),
                ),
            )
        )
    return snapshots


def build_formulaic_features_and_bars() -> tuple[list[FeatureVector], list[Bar]]:
    bars = build_microstructure_bars()
    forward_lookup: dict[tuple[str, datetime], float] = {}
    by_symbol: dict[str, list[Bar]] = {}
    for bar in bars:
        by_symbol.setdefault(bar.instrument.symbol, []).append(bar)
    for symbol_bars in by_symbol.values():
        for index in range(len(symbol_bars) - 1):
            current = symbol_bars[index]
            future = symbol_bars[index + 1]
            forward_lookup[(current.instrument.symbol, current.timestamp)] = (future.close / current.close) - 1.0
    features = [
        FeatureVector(
            as_of=bar.timestamp,
            instrument=bar.instrument,
            values={
                "signal": forward_lookup.get((bar.instrument.symbol, bar.timestamp), 0.0),
                "noise": (hash((bar.instrument.symbol, bar.timestamp.minute)) % 17) / 100.0,
            },
        )
        for bar in bars[:-3]
    ]
    return features, bars


@unittest.skipUnless(has_optional_dependency("pyarrow"), "pyarrow not installed")
class CuratedFeatureDatasetTestCase(unittest.TestCase):
    def test_curated_builder_and_store_round_trip(self) -> None:
        bars = build_microstructure_bars()
        trades = build_microstructure_trades(bars)
        books = build_microstructure_books(bars)
        frame = BinanceCuratedFeatureBuilder(lookbacks=(3, 5)).build(bars, trades, books)

        self.assertTrue(frame.features)
        self.assertIn("signed_quantity_imbalance", frame.features[0].values)
        self.assertIn("orderbook_spread_bps", frame.features[0].values)
        self.assertIn("orderbook_pressure", frame.features[0].values)
        self.assertIn("orderbook_liquidity_score", frame.features[0].values)
        self.assertIn("vpin_proxy", frame.features[0].values)
        self.assertIn("trade_sign_autocorrelation", frame.features[0].values)
        self.assertIn("volume_profile_tilt", frame.features[0].values)
        self.assertIn("volume_profile_entropy", frame.features[0].values)
        self.assertIn("volume_profile_poc_distance", frame.features[0].values)

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            persisted = CuratedFeatureDatasetService(
                store=ParquetFeatureFrameStore(),
                catalog=catalog,
            ).persist(
                frame,
                dataset_name="test.curated.microstructure.1m",
                version="2024-01-01_2024-01-01",
                storage_path=root / "curated",
                metadata={"source": "unit_test"},
            )
            loaded = ParquetFeatureFrameStore().read(persisted.dataset)

            self.assertEqual(len(loaded), len(frame.features))
            self.assertEqual(persisted.dataset.data_kind.value, "feature_frame")


class FormulaicMinerTestCase(unittest.TestCase):
    def test_gp_and_rl_miners_return_candidates(self) -> None:
        features, bars = build_formulaic_features_and_bars()
        gp = GeneticProgrammingFactorMiner(
            GeneticProgrammingConfig(population_size=20, generations=4, max_depth=3, top_k=4, random_seed=5)
        )
        rl = PolicyGradientFactorMiner(
            PolicyGradientConfig(episodes=4, samples_per_episode=12, max_depth=3, top_k=4, random_seed=9)
        )

        gp_result = gp.mine(features, bars)
        rl_result = rl.mine(features, bars)

        self.assertTrue(gp_result.candidates)
        self.assertTrue(rl_result.candidates)
        self.assertGreater(max(candidate.fitness for candidate in gp_result.candidates), -1.0)
        self.assertGreater(max(candidate.fitness for candidate in rl_result.candidates), -1.0)


@unittest.skipUnless(has_optional_dependency("pyarrow"), "pyarrow not installed")
class FormulaicWorkflowTestCase(unittest.TestCase):
    def test_formulaic_workflow_and_batch_sweep_log_experiments(self) -> None:
        features, bars = build_formulaic_features_and_bars()
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            factor_store = LocalFactorStore(FactorCatalog(root / "factor_catalog.json"))
            tracker = LocalExperimentTracker(root / "experiments")
            workflow = FormulaicFactorWorkflow(
                miner=GeneticProgrammingFactorMiner(
                    GeneticProgrammingConfig(population_size=20, generations=4, max_depth=3, top_k=4, random_seed=13)
                ),
                selector=ThresholdFactorSelector(
                    FactorSelectionPolicy(min_mean_ic=0.0, min_hit_rate=0.4, min_cross_sections=2, min_mean_quantile_spread=-1.0)
                ),
                combiner=ICWeightFactorCombiner(),
                backtester=QuantileLongShortBacktester(periods_per_year=365),
                factor_store=factor_store,
                artifact_base_path=root / "artifacts",
                artifact_name="formulaic_unit_test",
                tracker=tracker,
            )

            result = workflow.run(features, bars, version="2024-01-01", metadata={"source": "unit_test"})

            self.assertTrue(result.factor_search.candidates)
            self.assertIsNotNone(result.experiment_path)
            self.assertTrue(result.experiment_path.exists())
            self.assertTrue(result.artifacts)

            sweep = FormulaicMiningSweepRunner().run(
                entries=[
                    MiningSweepEntry(
                        label="gp",
                        workflow_builder=lambda: FormulaicFactorWorkflow(
                            miner=GeneticProgrammingFactorMiner(
                                GeneticProgrammingConfig(population_size=16, generations=3, max_depth=3, top_k=3, random_seed=21)
                            ),
                            selector=ThresholdFactorSelector(
                                FactorSelectionPolicy(min_mean_ic=0.0, min_hit_rate=0.4, min_cross_sections=2, min_mean_quantile_spread=-1.0)
                            ),
                            combiner=ICWeightFactorCombiner(),
                            backtester=QuantileLongShortBacktester(periods_per_year=365),
                            factor_store=factor_store,
                            artifact_base_path=root / "artifacts_gp",
                            artifact_name="gp_sweep",
                            tracker=tracker,
                        ),
                    ),
                    MiningSweepEntry(
                        label="rl",
                        workflow_builder=lambda: FormulaicFactorWorkflow(
                            miner=PolicyGradientFactorMiner(
                                PolicyGradientConfig(episodes=3, samples_per_episode=10, max_depth=3, top_k=3, random_seed=23)
                            ),
                            selector=ThresholdFactorSelector(
                                FactorSelectionPolicy(min_mean_ic=0.0, min_hit_rate=0.4, min_cross_sections=2, min_mean_quantile_spread=-1.0)
                            ),
                            combiner=ICWeightFactorCombiner(),
                            backtester=QuantileLongShortBacktester(periods_per_year=365),
                            factor_store=factor_store,
                            artifact_base_path=root / "artifacts_rl",
                            artifact_name="rl_sweep",
                            tracker=tracker,
                        ),
                    ),
                ],
                features=features,
                bars=bars,
                version_prefix="sweep",
                metadata={"source": "unit_test"},
            )

            self.assertEqual(len(sweep.runs), 2)
            self.assertEqual(len(tracker.compare_runs("best_fitness")), 3)


if __name__ == "__main__":
    unittest.main()
