from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from quantlab.core.enums import AssetClass, Side
from quantlab.core.models import Bar, Instrument, PortfolioSnapshot, TargetPosition, Trade
from quantlab.data.catalog import DataCatalog
from quantlab.data.stores import ParquetMarketDataStore
from quantlab.portfolio.construction import LiquidityAwareAllocator, RegimeAwareAllocator
from quantlab.research.curation import BinanceCuratedFeatureBuilder
from quantlab.research.loaders import HistoricalOrderBookLoader
from quantlab.research.orderbook import SyntheticDepthDatasetService, SyntheticDepthSnapshotBuilder
from quantlab.research.regime import RegimeSignal
from quantlab.risk.policies import LiquidityParticipationPolicy, RegimeStateLimitPolicy, RiskPolicyStack


def has_optional_dependency(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def build_minute_bars_and_trades() -> tuple[list[Bar], list[Trade]]:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    closes_by_symbol = {
        "BTCUSDT": [42000.0, 42020.0, 42060.0, 42120.0, 42080.0],
        "ETHUSDT": [2200.0, 2204.0, 2210.0, 2218.0, 2212.0],
    }
    bars: list[Bar] = []
    trades: list[Trade] = []
    for symbol, closes in closes_by_symbol.items():
        instrument = Instrument(symbol=symbol, venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        for index, close in enumerate(closes):
            timestamp = start + timedelta(minutes=index)
            bar = Bar(
                timestamp=timestamp,
                instrument=instrument,
                open=close - 8.0,
                high=close + 12.0,
                low=close - 12.0,
                close=close,
                volume=120.0 + (index * 15.0),
            )
            bars.append(bar)
            for trade_index, side in enumerate((Side.BUY, Side.BUY, Side.SELL, Side.BUY, Side.SELL, Side.BUY)):
                direction = 1.0 if side is Side.BUY else -1.0
                trades.append(
                    Trade(
                        timestamp=timestamp + timedelta(seconds=trade_index * 8),
                        instrument=instrument,
                        trade_id=f"{symbol}-{index}-{trade_index}",
                        price=close + (direction * (0.3 + trade_index * 0.05)),
                        quantity=0.25 + (trade_index * 0.08),
                        side=side,
                    )
                )
    return bars, trades


class SyntheticDepthSnapshotBuilderTestCase(unittest.TestCase):
    def test_builds_proxy_depth_snapshots_from_bars_and_trades(self) -> None:
        bars, trades = build_minute_bars_and_trades()
        snapshots = SyntheticDepthSnapshotBuilder().build(bars, trades)

        self.assertEqual(len(snapshots), len(bars))
        self.assertEqual(len(snapshots[0].bids), 5)
        self.assertEqual(len(snapshots[0].asks), 5)
        self.assertEqual(snapshots[0].metadata["source"], "synthetic_depth_proxy")
        self.assertGreater(snapshots[0].bids[0].quantity, 0.0)
        self.assertGreater(snapshots[0].asks[0].price, snapshots[0].bids[0].price)


@unittest.skipUnless(has_optional_dependency("pyarrow"), "pyarrow not installed")
class SyntheticDepthDatasetServiceTestCase(unittest.TestCase):
    def test_materializes_and_loads_synthetic_depth_datasets(self) -> None:
        bars, trades = build_minute_bars_and_trades()
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            store = ParquetMarketDataStore()
            result = SyntheticDepthDatasetService(store=store, catalog=catalog).materialize_binance_range(
                bars,
                trades,
                interval="1m",
                storage_path=root / "curated",
            )

            self.assertEqual(len(result.datasets), 2)
            self.assertEqual(len(result.snapshots), len(bars))
            loaded = HistoricalOrderBookLoader(catalog, store).load_binance_synthetic_depth(
                ("BTCUSDT", "ETHUSDT"),
                interval="1m",
                start_version="2024-01-01",
                end_version="2024-01-01",
            )
            self.assertEqual(len(loaded), len(result.snapshots))

            frame = BinanceCuratedFeatureBuilder(lookbacks=(3, 5)).build(bars, trades, loaded)
            self.assertTrue(frame.features)
            self.assertIn("orderbook_pressure", frame.features[0].values)
            self.assertIn("orderbook_liquidity_score", frame.features[0].values)
            self.assertIn("volume_profile_entropy", frame.features[0].values)


class RegimeAwareOverlayTestCase(unittest.TestCase):
    def test_regime_allocator_and_risk_stack_reduce_gross_and_position_size(self) -> None:
        as_of = datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc)
        btc = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        eth = Instrument(symbol="ETHUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        portfolio = PortfolioSnapshot(timestamp=as_of, cash=1_000_000.0)
        targets = (
            TargetPosition(
                as_of=as_of,
                instrument=btc,
                target_weight=0.70,
                reason="preview",
                signal_name="alpha",
                metadata={"mark_price": "42000", "adv_notional": "50000000", "expected_cost_bps": "4.0"},
            ),
            TargetPosition(
                as_of=as_of,
                instrument=eth,
                target_weight=-0.55,
                reason="preview",
                signal_name="alpha",
                metadata={"mark_price": "2200", "adv_notional": "150000", "expected_cost_bps": "18.0"},
            ),
        )
        regime_signal = RegimeSignal(
            as_of=as_of,
            scope="crypto_market",
            state_id=2,
            confidence=0.91,
            transition_probabilities={0: 0.10, 1: 0.25, 2: 0.65},
        )

        allocated = RegimeAwareAllocator(
            LiquidityAwareAllocator(
                gross_target=1.0,
                max_abs_weight=0.50,
                max_turnover=1.0,
                max_adv_fraction=0.05,
                cost_penalty_bps=12.0,
            ),
            gross_target_by_state={0: 1.0, 1: 0.75, 2: 0.40},
        ).allocate(targets, portfolio, regime_signal)

        allocated_gross = sum(abs(target.target_weight) for target in allocated)
        self.assertLessEqual(allocated_gross, 0.400001)
        self.assertLessEqual(abs(next(target for target in allocated if target.instrument.symbol == "ETHUSDT").target_weight), 0.01)

        approved = RiskPolicyStack(
            [
                LiquidityParticipationPolicy(max_adv_fraction=0.05),
                RegimeStateLimitPolicy(
                    max_position_weight_by_state={2: 0.12},
                    max_gross_by_state={2: 0.25},
                ),
            ]
        ).apply(allocated, portfolio, regime_signal)

        self.assertLessEqual(sum(abs(target.target_weight) for target in approved), 0.250001)
        self.assertTrue(all(abs(target.target_weight) <= 0.120001 for target in approved))


if __name__ == "__main__":
    unittest.main()
