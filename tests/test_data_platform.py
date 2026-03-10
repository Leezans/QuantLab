from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from quantlab.core.enums import AssetClass, DataFrequency, DatasetKind
from quantlab.core.models import Bar, Instrument
from quantlab.data import DataCatalog, DuckDBQueryService, IngestionRequest, MarketDataIngestionService, ParquetBarStore


def has_optional_dependency(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def build_sample_bars() -> list[Bar]:
    instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    closes = [100.0, 101.0, 103.0, 102.0]
    return [
        Bar(
            timestamp=start + timedelta(days=index),
            instrument=instrument,
            open=close - 1.0,
            high=close + 1.0,
            low=close - 2.0,
            close=close,
            volume=1_000.0 + index,
            metadata={"interval": "1d", "source": "unit_test"},
        )
        for index, close in enumerate(closes)
    ]


class StaticMarketDataSource:
    def __init__(self, bars: list[Bar]) -> None:
        self._bars = list(bars)

    def fetch_bars(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> list[Bar]:
        del frequency
        return [
            bar
            for bar in self._bars
            if bar.instrument.symbol == instrument.symbol and start <= bar.timestamp <= end
        ]


@unittest.skipUnless(has_optional_dependency("pyarrow"), "pyarrow not installed")
class ParquetStoreTestCase(unittest.TestCase):
    def test_catalog_persists_dataset_metadata(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            catalog_path = root / "catalog" / "catalog.json"
            dataset_path = root / "warehouse" / "bars" / "v1"
            catalog = DataCatalog(catalog_path)
            bars = build_sample_bars()
            dataset = IngestionRequest(
                dataset_name="bars",
                version="v1",
                instrument=bars[0].instrument,
                start=bars[0].timestamp,
                end=bars[-1].timestamp,
                frequency=DataFrequency.DAILY,
                storage_path=root / "warehouse",
            )
            service = MarketDataIngestionService(
                source=StaticMarketDataSource(bars),
                store=ParquetBarStore(),
                catalog=catalog,
            )

            ref = service.ingest(dataset)

            self.assertEqual(ref.location, dataset_path)
            reloaded = DataCatalog(catalog_path)
            resolved = reloaded.resolve("bars", "v1")
            self.assertEqual(resolved.row_count, 4)
            self.assertEqual(resolved.partition_columns, ("symbol", "date"))

    def test_parquet_store_round_trip(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            bars = build_sample_bars()
            from quantlab.data.catalog import DatasetRef

            dataset = DatasetRef(
                name="bars",
                version="v1",
                data_kind=DatasetKind.BAR,
                asset_class=AssetClass.CRYPTO,
                location=root / "warehouse" / "bars" / "v1",
                schema=("timestamp", "symbol", "close"),
                row_count=len(bars),
                partition_columns=("symbol", "date"),
            )
            store = ParquetBarStore()
            store.write_bars(dataset, bars)
            round_trip = store.read_bars(dataset)
            self.assertEqual(len(round_trip), len(bars))
            self.assertEqual(round_trip[-1].close, bars[-1].close)
            self.assertEqual(round_trip[0].metadata["interval"], "1d")
            self.assertEqual(round_trip[0].metadata["source"], "unit_test")


@unittest.skipUnless(has_optional_dependency("duckdb") and has_optional_dependency("pyarrow"), "duckdb/pyarrow not installed")
class DuckDBQueryServiceTestCase(unittest.TestCase):
    def test_query_service_scans_partitioned_dataset(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            bars = build_sample_bars()
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            service = MarketDataIngestionService(
                source=StaticMarketDataSource(bars),
                store=ParquetBarStore(),
                catalog=catalog,
            )
            dataset = service.ingest(
                IngestionRequest(
                    dataset_name="bars",
                    version="v1",
                    instrument=bars[0].instrument,
                    start=bars[0].timestamp,
                    end=bars[-1].timestamp,
                    frequency=DataFrequency.DAILY,
                    storage_path=root / "warehouse",
                )
            )

            rows = DuckDBQueryService(root / "artifacts" / "quantlab.duckdb").query_dataset(
                dataset,
                "select symbol, count(*) as rows, avg(close) as avg_close from dataset group by symbol",
            )

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "BTCUSDT")
            self.assertEqual(rows[0]["rows"], 4)
            self.assertGreater(rows[0]["avg_close"], 100.0)


if __name__ == "__main__":
    unittest.main()
