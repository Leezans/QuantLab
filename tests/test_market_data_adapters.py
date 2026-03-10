from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from quantlab.core.enums import AssetClass, DataFrequency, DatasetKind, Side
from quantlab.core.models import Instrument
from quantlab.data import (
    BinanceMarketDataAdapter,
    DataCatalog,
    DuckDBQueryService,
    IngestionRequest,
    OrderBookSnapshotIngestionService,
    ParquetMarketDataStore,
    QuoteIngestionService,
    TradeIngestionService,
)


def has_optional_dependency(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


class StaticTradeDataSource:
    def __init__(self, trades: list) -> None:
        self._trades = trades

    def fetch_trades(self, instrument: Instrument, start: datetime, end: datetime) -> list:
        return [
            trade
            for trade in self._trades
            if trade.instrument.symbol == instrument.symbol and start <= trade.timestamp <= end
        ]


class StaticQuoteDataSource:
    def __init__(self, quotes: list) -> None:
        self._quotes = quotes

    def fetch_quotes(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> list:
        del frequency
        return [
            quote
            for quote in self._quotes
            if quote.instrument.symbol == instrument.symbol and start <= quote.timestamp <= end
        ]


class StaticOrderBookDataSource:
    def __init__(self, snapshots: list) -> None:
        self._snapshots = snapshots

    def fetch_order_book_snapshots(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> list:
        del frequency
        return [
            snapshot
            for snapshot in self._snapshots
            if snapshot.instrument.symbol == instrument.symbol and start <= snapshot.timestamp <= end
        ]


class BinanceAdapterTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO)
        self.adapter = BinanceMarketDataAdapter()

    def test_normalizes_agg_trades(self) -> None:
        trades = self.adapter.normalize_agg_trades(
            [
                {"a": 1, "p": "100.10", "q": "0.50", "T": 1704067200000, "m": False},
                {"a": 2, "p": "100.20", "q": "0.25", "T": 1704067201000, "m": True},
            ],
            self.instrument,
        )
        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].side, Side.BUY)
        self.assertEqual(trades[1].side, Side.SELL)
        self.assertAlmostEqual(trades[0].notional, 50.05)

    def test_normalizes_depth_updates(self) -> None:
        snapshots = self.adapter.normalize_depth_updates(
            [
                {"E": 1704067200000, "u": 101, "b": [["100.00", "1.50"]], "a": [["100.15", "1.20"]]},
            ],
            self.instrument,
        )
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].bids[0].side, Side.BUY)
        self.assertEqual(snapshots[0].asks[0].side, Side.SELL)
        self.assertEqual(snapshots[0].sequence_id, "101")


@unittest.skipUnless(has_optional_dependency("duckdb") and has_optional_dependency("pyarrow"), "duckdb/pyarrow not installed")
class MicrostructureDatasetTestCase(unittest.TestCase):
    def test_ingests_and_queries_trades_quotes_and_books(self) -> None:
        instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO)
        adapter = BinanceMarketDataAdapter()
        trades = list(
            adapter.normalize_agg_trades(
                [
                    {"a": 1, "p": "100.10", "q": "0.50", "T": 1704067200000, "m": False},
                    {"a": 2, "p": "100.20", "q": "0.25", "T": 1704067201000, "m": True},
                ],
                instrument,
            )
        )
        quotes = list(
            adapter.normalize_book_ticker(
                [
                    {"T": 1704067200000, "b": "100.00", "B": "1.20", "a": "100.15", "A": "1.00"},
                    {"T": 1704067201000, "b": "100.05", "B": "1.30", "a": "100.20", "A": "0.90"},
                ],
                instrument,
            )
        )
        snapshots = list(
            adapter.normalize_depth_updates(
                [
                    {"E": 1704067200000, "u": 101, "b": [["100.00", "1.50"]], "a": [["100.15", "1.20"]]},
                    {"E": 1704067201000, "u": 102, "b": [["100.05", "1.30"]], "a": [["100.20", "0.90"]]},
                ],
                instrument,
            )
        )

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = ParquetMarketDataStore()
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            trade_dataset = TradeIngestionService(StaticTradeDataSource(trades), store, catalog).ingest(
                IngestionRequest(
                    dataset_name="trades",
                    version="v1",
                    instrument=instrument,
                    start=trades[0].timestamp,
                    end=trades[-1].timestamp,
                    storage_path=root / "warehouse",
                )
            )
            quote_dataset = QuoteIngestionService(StaticQuoteDataSource(quotes), store, catalog).ingest(
                IngestionRequest(
                    dataset_name="quotes",
                    version="v1",
                    instrument=instrument,
                    start=quotes[0].timestamp,
                    end=quotes[-1].timestamp,
                    storage_path=root / "warehouse",
                )
            )
            book_dataset = OrderBookSnapshotIngestionService(StaticOrderBookDataSource(snapshots), store, catalog).ingest(
                IngestionRequest(
                    dataset_name="books",
                    version="v1",
                    instrument=instrument,
                    start=snapshots[0].timestamp,
                    end=snapshots[-1].timestamp,
                    storage_path=root / "warehouse",
                )
            )

            self.assertEqual(trade_dataset.data_kind, DatasetKind.TRADE)
            self.assertEqual(quote_dataset.data_kind, DatasetKind.QUOTE)
            self.assertEqual(book_dataset.data_kind, DatasetKind.ORDER_BOOK_SNAPSHOT)

            query_service = DuckDBQueryService(root / "artifacts" / "quantlab.duckdb")
            trade_rows = query_service.query_dataset(
                trade_dataset,
                "select symbol, count(*) as rows, avg(price) as avg_price from dataset group by symbol",
            )
            quote_rows = query_service.query_dataset(
                quote_dataset,
                "select symbol, avg(spread) as avg_spread from dataset group by symbol",
            )
            book_rows = query_service.query_dataset(
                book_dataset,
                """
                select
                    count(distinct sequence_id) as snapshot_count,
                    avg(case when side = 'buy' and level = 1 then price end) as avg_best_bid
                from dataset
                """.strip(),
            )

            self.assertEqual(trade_rows[0]["rows"], 2)
            self.assertGreater(trade_rows[0]["avg_price"], 100.0)
            self.assertGreater(quote_rows[0]["avg_spread"], 0.0)
            self.assertEqual(book_rows[0]["snapshot_count"], 2)
            self.assertGreater(book_rows[0]["avg_best_bid"], 100.0)


if __name__ == "__main__":
    unittest.main()
