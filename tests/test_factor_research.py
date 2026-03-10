from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from quantlab.core.enums import AssetClass, DataFrequency, DatasetKind
from quantlab.core.models import Bar, Instrument
from quantlab.data import DataCatalog, ParquetMarketDataStore
from quantlab.data.ingestion import IngestionRequest, build_dataset_ref
from quantlab.orchestration.factor_pipeline import FactorResearchWorkflow
from quantlab.research.factor_backtest import QuantileLongShortBacktester
from quantlab.research.factor_combination import ICWeightFactorCombiner
from quantlab.research.factor_evaluation import FactorEvaluator
from quantlab.research.factor_orthogonalization import SequentialFactorOrthogonalizer
from quantlab.research.factor_selection import FactorSelectionPolicy, ThresholdFactorSelector
from quantlab.research.factor_storage import FactorCatalog, LocalFactorStore
from quantlab.research.factors import FactorDefinition, FactorExposure, FactorMiner, FactorNormalization
from quantlab.research.features import FeaturePipeline, make_trailing_return_feature
from quantlab.research.loaders import HistoricalBarLoader


def has_optional_dependency(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def build_multi_asset_bars() -> list[Bar]:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    trajectories = {
        "BTCUSDT": [100.0, 102.0, 104.0, 106.0, 109.0, 112.0, 115.0, 118.0, 122.0, 126.0, 130.0, 135.0],
        "ETHUSDT": [80.0, 81.0, 82.0, 83.0, 84.5, 85.0, 86.0, 87.0, 88.0, 89.0, 90.0, 91.0],
        "SOLUSDT": [50.0, 49.5, 49.0, 48.0, 47.0, 46.5, 46.0, 45.0, 44.0, 43.5, 43.0, 42.0],
        "BNBUSDT": [60.0, 61.0, 61.5, 62.0, 62.5, 63.5, 64.0, 65.0, 66.0, 67.0, 68.5, 70.0],
    }
    bars: list[Bar] = []
    for symbol, closes in trajectories.items():
        instrument = Instrument(symbol=symbol, venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        for offset, close in enumerate(closes):
            timestamp = start + timedelta(days=offset)
            bars.append(
                Bar(
                    timestamp=timestamp,
                    instrument=instrument,
                    open=close - 0.4,
                    high=close + 1.0,
                    low=close - 1.0,
                    close=close,
                    volume=1_000.0 + offset * 20.0,
                )
            )
    return bars


@unittest.skipUnless(has_optional_dependency("pyarrow"), "pyarrow not installed")
class HistoricalBarLoaderTestCase(unittest.TestCase):
    def test_load_binance_klines_across_symbols_and_versions(self) -> None:
        bars = build_multi_asset_bars()
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            store = ParquetMarketDataStore()
            for symbol in ("BTCUSDT", "ETHUSDT"):
                instrument = Instrument(symbol=symbol, venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
                for day_offset in (0, 1):
                    version = (datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_offset)).date().isoformat()
                    request = IngestionRequest(
                        dataset_name=f"binance.spot.klines.{symbol.lower()}.1d",
                        version=version,
                        instrument=instrument,
                        start=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_offset),
                        end=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_offset),
                        frequency=DataFrequency.DAILY,
                        storage_path=root / "warehouse",
                    )
                    dataset = build_dataset_ref(request, DatasetKind.BAR, row_count=1)
                    symbol_bars = [
                        bar
                        for bar in bars
                        if bar.instrument.symbol == symbol and bar.timestamp.date().isoformat() == version
                    ]
                    store.write_bars(dataset, symbol_bars)
                    catalog.register(dataset)

            loaded = HistoricalBarLoader(catalog, store).load_binance_klines(
                ["BTCUSDT", "ETHUSDT"],
                interval="1d",
                start_version="2024-01-01",
                end_version="2024-01-02",
            )

        self.assertEqual(len(loaded), 4)
        self.assertEqual({bar.instrument.symbol for bar in loaded}, {"BTCUSDT", "ETHUSDT"})


@unittest.skipUnless(has_optional_dependency("pyarrow"), "pyarrow not installed")
class FactorResearchWorkflowTestCase(unittest.TestCase):
    def test_factor_workflow_selects_and_persists_positive_factors(self) -> None:
        bars = build_multi_asset_bars()
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow = FactorResearchWorkflow(
                feature_pipeline=FeaturePipeline([make_trailing_return_feature("momentum_3", lookback=3)]),
                factor_miner=FactorMiner(
                    [
                        FactorDefinition("momentum_raw", "momentum_3", 1.0, FactorNormalization.RAW),
                        FactorDefinition("momentum_rank", "momentum_3", 1.0, FactorNormalization.RANK),
                        FactorDefinition("momentum_inv", "momentum_3", -1.0, FactorNormalization.RAW),
                    ]
                ),
                evaluator=FactorEvaluator(quantiles=2, min_observations=3),
                selector=ThresholdFactorSelector(
                    FactorSelectionPolicy(
                        min_mean_ic=0.5,
                        min_hit_rate=0.8,
                        min_cross_sections=4,
                        min_mean_quantile_spread=0.0,
                    )
                ),
                combiner=ICWeightFactorCombiner(),
                backtester=QuantileLongShortBacktester(periods_per_year=365),
                factor_store=LocalFactorStore(FactorCatalog(root / "artifacts" / "factor_catalog.json")),
                artifact_base_path=root / "artifacts" / "factor_lab",
                artifact_name="binance_factor_lab_test",
                forward_horizon=1,
                decay_horizons=(1, 3),
            )

            result = workflow.run(
                bars,
                version="2024-01-12",
                metadata={"universe": "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT"},
            )

            summary_lookup = {summary.factor_name: summary for summary in result.summaries}
            selected_names = {summary.factor_name for summary in result.selected_summaries}
            artifact_types = {artifact.artifact_type for artifact in result.artifacts}
            backtest_lookup = {backtest.factor_name: backtest for backtest in result.backtests}

            self.assertGreater(summary_lookup["momentum_raw"].mean_ic or 0.0, 0.9)
            self.assertLess(summary_lookup["momentum_inv"].mean_ic or 0.0, -0.9)
            self.assertIn("momentum_raw", selected_names)
            self.assertIn("momentum_rank", selected_names)
            self.assertNotIn("momentum_inv", selected_names)
            self.assertTrue(result.composite_exposures)
            self.assertTrue(result.combination_weights)
            self.assertIn("composite.ic_weighted", backtest_lookup)
            self.assertGreater(backtest_lookup["composite.ic_weighted"].report.cumulative_return, 0.0)
            self.assertEqual({point.horizon for point in result.decay}, {1, 3})
            self.assertTrue(
                {
                    "factor_values",
                    "factor_summaries",
                    "factor_cross_sections",
                    "factor_decay",
                    "factor_weights",
                    "factor_backtest_reports",
                    "factor_backtest_series",
                }.issubset(artifact_types)
            )
            self.assertTrue((root / "artifacts" / "factor_catalog.json").exists())

    def test_sequential_orthogonalizer_residualizes_collinear_factor(self) -> None:
        instrument_a = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        instrument_b = Instrument(symbol="ETHUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        as_of = datetime(2024, 1, 1, tzinfo=timezone.utc)
        exposures = [
            FactorExposure("base", as_of, instrument_a, 1.0, "base", FactorNormalization.RAW),
            FactorExposure("base", as_of, instrument_b, 2.0, "base", FactorNormalization.RAW),
            FactorExposure("copy", as_of, instrument_a, 2.0, "copy", FactorNormalization.RAW),
            FactorExposure("copy", as_of, instrument_b, 4.0, "copy", FactorNormalization.RAW),
        ]

        orthogonalized = SequentialFactorOrthogonalizer().orthogonalize(exposures, factor_order=["base", "copy"])
        residuals = [item.value for item in orthogonalized if item.factor_name == "copy__ortho"]

        self.assertEqual(len(residuals), 2)
        self.assertTrue(all(abs(value) < 1e-6 for value in residuals))


if __name__ == "__main__":
    unittest.main()
