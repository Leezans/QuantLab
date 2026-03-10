from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from quantlab.core.enums import AssetClass, DatasetKind, Side, StorageTier
from quantlab.core.models import Bar, Instrument, OrderBookLevel, OrderBookSnapshot, Quote, Trade
from quantlab.data import BinanceRESTMarketDataClient, DataCatalog, DatasetRef, ParquetMarketDataStore
from quantlab.live import (
    BinanceGapFillService,
    BinanceRealtimeChannel,
    BinanceStitchedMarketDataService,
    BinanceStreamSubscription,
    IntradayCacheReconciliationService,
    IntradayEventPersistenceService,
)


class FakeJSONResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


class FakeRESTSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any], timeout: float):  # type: ignore[no-untyped-def]
        self.calls.append((url, dict(params)))
        if url.endswith("/api/v3/aggTrades"):
            if "fromId" in params:
                return FakeJSONResponse([])
            return FakeJSONResponse(
                [
                    {"a": 1001, "p": "42000.10", "q": "0.005", "T": 1704067200000, "m": False},
                    {"a": 1002, "p": "42001.10", "q": "0.010", "T": 1704067201000, "m": True},
                ]
            )
        if url.endswith("/api/v3/klines"):
            return FakeJSONResponse(
                [
                    [1704067200000, "42000.00", "42010.00", "41990.00", "42005.00", "10.00", 1704067259999, "420000.00", 100, "4.00", "168000.00", "0"],
                    [1704067260000, "42005.00", "42020.00", "42000.00", "42015.00", "8.50", 1704067319999, "357000.00", 80, "3.50", "147000.00", "0"],
                ]
            )
        if url.endswith("/api/v3/ticker/bookTicker"):
            return FakeJSONResponse(
                {
                    "symbol": "BTCUSDT",
                    "bidPrice": "42000.00",
                    "bidQty": "1.20",
                    "askPrice": "42000.25",
                    "askQty": "0.80",
                }
            )
        if url.endswith("/api/v3/depth"):
            return FakeJSONResponse(
                {
                    "lastUpdateId": 8001,
                    "bids": [["42000.00", "1.50"], ["41999.90", "2.00"]],
                    "asks": [["42000.25", "0.80"], ["42000.40", "1.20"]],
                }
            )
        raise AssertionError(f"unexpected URL {url}")


class BinanceRESTMarketDataClientTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.client = BinanceRESTMarketDataClient(session=FakeRESTSession())
        self.start = datetime(2024, 1, 1, tzinfo=UTC)
        self.end = self.start + timedelta(minutes=2)

    def test_fetches_gap_fill_market_data(self) -> None:
        trades = self.client.fetch_agg_trades("BTCUSDT", self.start, self.end)
        bars = self.client.fetch_klines("BTCUSDT", "1m", self.start, self.end)
        quote = self.client.fetch_book_ticker("BTCUSDT", as_of=self.end)
        book = self.client.fetch_order_book_snapshot("BTCUSDT", as_of=self.end, limit=5)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].instrument.quote_currency, "USDT")
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[0].metadata["interval"], "1m")
        self.assertIsInstance(quote, Quote)
        self.assertEqual(book.sequence_id, "8001")
        self.assertEqual(book.bids[0].price, 42000.0)


class IntradayEventPersistenceServiceTestCase(unittest.TestCase):
    def test_persists_and_merges_intraday_events(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
            persistence = IntradayEventPersistenceService(
                store=ParquetMarketDataStore(),
                catalog=DataCatalog(root / "catalog" / "catalog.json"),
                base_path=root / "cache",
            )

            first = Trade(
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                instrument=instrument,
                trade_id="1001",
                price=42000.0,
                quantity=0.01,
                metadata={"source": "ws"},
            )
            duplicate = Trade(
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                instrument=instrument,
                trade_id="1001",
                price=42000.0,
                quantity=0.01,
                metadata={"source": "ws"},
            )
            second = Trade(
                timestamp=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                instrument=instrument,
                trade_id="1002",
                price=42001.0,
                quantity=0.02,
                metadata={"source": "ws"},
            )

            dataset = persistence.persist_event(first, source="binance_ws")
            dataset = persistence.persist_events((duplicate, second), source="binance_ws")[0]

            self.assertEqual(dataset.row_count, 2)
            self.assertEqual(dataset.name, "binance.spot.cache.trades.btcusdt")
            self.assertEqual(dataset.storage_tier, StorageTier.CACHE)
            self.assertIn("/cache/", str(dataset.location))
            round_trip = ParquetMarketDataStore().read_trades(dataset)
            self.assertEqual(len(round_trip), 2)
            self.assertEqual(round_trip[0].metadata["source"], "ws")


class FakeGapFillRestClient:
    def __init__(self, trade: Trade, bar: Bar) -> None:
        self.trade = trade
        self.bar = bar

    def fetch_agg_trades(self, symbol: str, start: datetime, end: datetime) -> tuple[Trade, ...]:
        del symbol, start, end
        return (self.trade,)

    def fetch_klines(self, symbol: str, interval: str, start: datetime, end: datetime) -> tuple[Bar, ...]:
        del symbol, interval, start, end
        return (self.bar,)

    def fetch_book_ticker(self, symbol: str, as_of: datetime | None = None) -> Quote:
        del symbol, as_of
        raise AssertionError("bookTicker not expected in this test")

    def fetch_order_book_snapshot(self, symbol: str, limit: int = 20, as_of: datetime | None = None) -> OrderBookSnapshot:
        del symbol, limit, as_of
        raise AssertionError("depth not expected in this test")


class BinanceStitchedMarketDataServiceTestCase(unittest.TestCase):
    def test_primes_gap_fill_and_filters_stale_trade_events(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
            trade = Trade(
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                instrument=instrument,
                trade_id="1001",
                price=42000.0,
                quantity=0.01,
                metadata={"source": "rest"},
            )
            bar = Bar(
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                instrument=instrument,
                open=42000.0,
                high=42010.0,
                low=41990.0,
                close=42005.0,
                volume=10.0,
                metadata={"interval": "1m", "event_time": "1704067200000"},
            )
            persistence = IntradayEventPersistenceService(
                store=ParquetMarketDataStore(),
                catalog=DataCatalog(root / "catalog" / "catalog.json"),
                base_path=root / "cache",
            )
            gap_fill = BinanceGapFillService(FakeGapFillRestClient(trade=trade, bar=bar), persistence)
            stitcher = BinanceStitchedMarketDataService(gap_fill=gap_fill, persistence=persistence)

            primed = stitcher.prime(
                subscriptions=[
                    BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.AGG_TRADE),
                    BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.KLINE, interval="1m"),
                ],
                start=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                end=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            )
            self.assertEqual(len(primed), 2)

            duplicate_trade = Trade(
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                instrument=instrument,
                trade_id="1001",
                price=42000.0,
                quantity=0.01,
            )
            fresh_trade = Trade(
                timestamp=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                instrument=instrument,
                trade_id="1002",
                price=42001.0,
                quantity=0.02,
            )
            stale_result = stitcher.consume_event(duplicate_trade)
            fresh_result = stitcher.consume_event(fresh_trade)

            self.assertIsNone(stale_result)
            self.assertIsNotNone(fresh_result)
            self.assertEqual(fresh_result.row_count, 2)

    def test_accepts_newer_kline_update_for_same_bar(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
            initial_bar = Bar(
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                instrument=instrument,
                open=42000.0,
                high=42010.0,
                low=41990.0,
                close=42005.0,
                volume=10.0,
                metadata={"interval": "1m", "event_time": "1704067200000"},
            )
            persistence = IntradayEventPersistenceService(
                store=ParquetMarketDataStore(),
                catalog=DataCatalog(root / "catalog" / "catalog.json"),
                base_path=root / "cache",
            )
            gap_fill = BinanceGapFillService(FakeGapFillRestClient(trade=Trade(
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument=instrument,
                trade_id="0",
                price=1.0,
                quantity=1.0,
            ), bar=initial_bar), persistence)
            stitcher = BinanceStitchedMarketDataService(gap_fill=gap_fill, persistence=persistence)
            stitcher.prime(
                subscriptions=[BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.KLINE, interval="1m")],
                start=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                end=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            )

            updated_bar = Bar(
                timestamp=initial_bar.timestamp,
                instrument=instrument,
                open=42000.0,
                high=42020.0,
                low=41990.0,
                close=42015.0,
                volume=12.0,
                metadata={"interval": "1m", "event_time": "1704067205000"},
            )
            dataset = stitcher.consume_event(updated_bar)
            self.assertIsNotNone(dataset)
            self.assertEqual(dataset.row_count, 1)

            round_trip = ParquetMarketDataStore().read_bars(dataset)
            self.assertEqual(round_trip[0].close, 42015.0)
            self.assertEqual(round_trip[0].metadata["event_time"], "1704067205000")


class IntradayCacheReconciliationServiceTestCase(unittest.TestCase):
    def test_purges_trade_cache_when_historical_trade_dataset_exists(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            store = ParquetMarketDataStore()
            persistence = IntradayEventPersistenceService(store=store, catalog=catalog, base_path=root / "cache")

            cached_dataset = persistence.persist_event(
                Trade(
                    timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                    instrument=instrument,
                    trade_id="1001",
                    price=42000.0,
                    quantity=0.01,
                ),
                source="binance_ws",
            )

            historical_dataset = DatasetRef(
                name="binance.spot.aggTrades.btcusdt",
                version="2024-01-01",
                data_kind=DatasetKind.TRADE,
                asset_class=AssetClass.CRYPTO,
                location=root / "warehouse" / "binance.spot.aggTrades.btcusdt" / "2024-01-01",
                schema=("timestamp", "symbol", "trade_id"),
                storage_tier=StorageTier.NORMALIZED,
                row_count=1,
            )
            catalog.register(historical_dataset)

            result = IntradayCacheReconciliationService(
                store=store,
                catalog=catalog,
                cache_persistence=persistence,
                historical_base_path=root / "warehouse",
            ).reconcile(version="2024-01-01")

            self.assertEqual(len(result.purged_trade_caches), 1)
            self.assertEqual(result.purged_trade_caches[0].name, cached_dataset.name)
            self.assertFalse(cached_dataset.location.exists())
            self.assertIsNone(catalog.get(cached_dataset.name, cached_dataset.version))

    def test_archives_order_book_cache_into_historical_warehouse(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            store = ParquetMarketDataStore()
            persistence = IntradayEventPersistenceService(store=store, catalog=catalog, base_path=root / "cache")

            cached_dataset = persistence.persist_event(
                OrderBookSnapshot(
                    timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                    instrument=instrument,
                    sequence_id="7001",
                    bids=(OrderBookLevel(side=Side.BUY, level=1, price=42000.0, quantity=1.0),),
                    asks=(OrderBookLevel(side=Side.SELL, level=1, price=42000.5, quantity=1.1),),
                    metadata={"depth": "5"},
                ),
                source="binance_ws",
            )

            result = IntradayCacheReconciliationService(
                store=store,
                catalog=catalog,
                cache_persistence=persistence,
                historical_base_path=root / "warehouse",
            ).reconcile(version="2024-01-01")

            self.assertEqual(len(result.archived_cache_datasets), 1)
            archived_dataset = result.archived_cache_datasets[0]
            self.assertEqual(archived_dataset.storage_tier, StorageTier.NORMALIZED)
            self.assertEqual(archived_dataset.name, "binance.spot.archive.order_book.btcusdt")
            self.assertTrue(archived_dataset.location.exists())
            self.assertEqual(archived_dataset.row_count, 2)
            self.assertIsNone(catalog.get(cached_dataset.name, cached_dataset.version))
            self.assertIsNotNone(catalog.get(archived_dataset.name, archived_dataset.version))


if __name__ == "__main__":
    unittest.main()
