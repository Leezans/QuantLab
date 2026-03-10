from __future__ import annotations

import hashlib
import importlib.util
import io
import unittest
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

from quantlab.data import (
    BinanceDataset,
    BinanceFrequency,
    BinanceHistoryEnsurer,
    BinanceHistoricalImporter,
    BinanceHistoricalParser,
    BinanceHistoricalSpec,
    BinanceMarket,
    BinanceVisionClient,
    DataCatalog,
    DuckDBQueryService,
    ParquetMarketDataStore,
    RawArtifactStore,
)


def has_optional_dependency(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


class FakeResponse:
    def __init__(self, payload: bytes | str, status_code: int = 200) -> None:
        if isinstance(payload, bytes):
            self.content = payload
            self.text = payload.decode("utf-8", errors="ignore")
        else:
            self.content = payload.encode("utf-8")
            self.text = payload
        self.encoding = "utf-8"
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise FakeHTTPError(self.status_code)
        return None


class FakeHTTPError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"http status {status_code}")
        self.response = type("Response", (), {"status_code": status_code})()


class FakeSession:
    def __init__(self, responses: dict[str, bytes | str | tuple[int, bytes | str]]) -> None:
        self._responses = dict(responses)

    def get(self, url: str, timeout: float) -> FakeResponse:
        del timeout
        try:
            payload = self._responses[url]
        except KeyError as exc:
            raise AssertionError(f"unexpected URL requested: {url}") from exc
        if isinstance(payload, tuple):
            status_code, body = payload
            return FakeResponse(body, status_code=status_code)
        return FakeResponse(payload)


def build_zip_bytes(filename: str, body: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, body)
    return buffer.getvalue()


class BinanceVisionClientTestCase(unittest.TestCase):
    def test_normalized_dataset_name_for_klines_includes_interval(self) -> None:
        spec = BinanceHistoricalSpec(
            market=BinanceMarket.SPOT,
            frequency=BinanceFrequency.DAILY,
            dataset=BinanceDataset.KLINES,
            symbol="BTCUSDT",
            date="2023-01-01",
            interval="1d",
        )
        self.assertEqual(spec.normalized_dataset_name(), "binance.spot.klines.btcusdt.1d")

    def test_remote_url_for_trades(self) -> None:
        client = BinanceVisionClient(session=FakeSession({}))
        spec = BinanceHistoricalSpec(
            market=BinanceMarket.SPOT,
            frequency=BinanceFrequency.DAILY,
            dataset=BinanceDataset.TRADES,
            symbol="BTCUSDT",
            date="2023-01-01",
        )
        self.assertEqual(
            client.remote_url(spec),
            "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-01-01.zip",
        )

    def test_download_to_raw_store_writes_data_checksum_and_manifest(self) -> None:
        spec = BinanceHistoricalSpec(
            market=BinanceMarket.SPOT,
            frequency=BinanceFrequency.DAILY,
            dataset=BinanceDataset.TRADES,
            symbol="BTCUSDT",
            date="2023-01-01",
        )
        zip_bytes = build_zip_bytes(
            "BTCUSDT-trades-2023-01-01.csv",
            "1,100.1,0.5,50.05,1672531200000,false,true\n",
        )
        checksum = hashlib.sha256(zip_bytes).hexdigest()
        client = BinanceVisionClient(
            session=FakeSession(
                {
                    "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-01-01.zip": zip_bytes,
                    "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-01-01.zip.CHECKSUM": f"{checksum}  BTCUSDT-trades-2023-01-01.zip",
                }
            )
        )

        with TemporaryDirectory() as temp_dir:
            raw_store = RawArtifactStore(Path(temp_dir))
            result = client.download_to_raw_store(raw_store, spec, fetch_checksum=True, verify=True)
            self.assertTrue(result.verified)
            self.assertTrue(result.artifact.data_path.exists())
            self.assertTrue(Path(result.checksum_path).exists())
            self.assertIsNotNone(result.artifact.manifest_path)
            self.assertTrue(result.artifact.manifest_path.exists())


class BinanceHistoricalParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = BinanceHistoricalParser()

    def test_parse_trade_zip_without_headers(self) -> None:
        spec = BinanceHistoricalSpec(
            market=BinanceMarket.SPOT,
            frequency=BinanceFrequency.DAILY,
            dataset=BinanceDataset.TRADES,
            symbol="BTCUSDT",
            date="2023-01-01",
        )
        zip_bytes = build_zip_bytes(
            "BTCUSDT-trades-2023-01-01.csv",
            "1,100.1,0.5,50.05,1672531200000,false,true\n2,100.2,0.25,25.05,1672531201000,true,true\n",
        )
        with TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "sample.zip"
            zip_path.write_bytes(zip_bytes)
            trades = self.parser.parse_trades(zip_path, spec)
        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].side.value, "buy")
        self.assertEqual(trades[1].side.value, "sell")

    def test_parse_kline_zip_without_headers(self) -> None:
        spec = BinanceHistoricalSpec(
            market=BinanceMarket.SPOT,
            frequency=BinanceFrequency.DAILY,
            dataset=BinanceDataset.KLINES,
            symbol="BTCUSDT",
            date="2023-01-01",
            interval="1d",
        )
        zip_bytes = build_zip_bytes(
            "BTCUSDT-1d-2023-01-01.csv",
            "1672531200000,100,110,90,105,10,1672617599999,1000,5,4,400,0\n",
        )
        with TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "sample.zip"
            zip_path.write_bytes(zip_bytes)
            bars = self.parser.parse_bars(zip_path, spec)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].close, 105.0)
        self.assertEqual(bars[0].instrument.symbol, "BTCUSDT")


@unittest.skipUnless(has_optional_dependency("duckdb") and has_optional_dependency("pyarrow"), "duckdb/pyarrow not installed")
class BinanceHistoricalImporterTestCase(unittest.TestCase):
    def test_importer_normalizes_trade_zip_into_parquet_dataset(self) -> None:
        spec = BinanceHistoricalSpec(
            market=BinanceMarket.SPOT,
            frequency=BinanceFrequency.DAILY,
            dataset=BinanceDataset.TRADES,
            symbol="BTCUSDT",
            date="2023-01-01",
        )
        zip_bytes = build_zip_bytes(
            "BTCUSDT-trades-2023-01-01.csv",
            "1,100.1,0.5,50.05,1672531200000,false,true\n2,100.2,0.25,25.05,1672531201000,true,true\n",
        )
        checksum = hashlib.sha256(zip_bytes).hexdigest()
        session = FakeSession(
            {
                "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-01-01.zip": zip_bytes,
                "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-01-01.zip.CHECKSUM": f"{checksum}  BTCUSDT-trades-2023-01-01.zip",
            }
        )

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            catalog = DataCatalog(root / "catalog" / "catalog.json")
            importer = BinanceHistoricalImporter(
                raw_store=RawArtifactStore(root / "raw"),
                dataset_store=ParquetMarketDataStore(),
                catalog=catalog,
                client=BinanceVisionClient(session=session),
            )

            dataset = importer.ingest(spec=spec, normalized_base_path=root / "normalized")
            self.assertEqual(dataset.data_kind.value, "trade")
            self.assertEqual(dataset.row_count, 2)
            self.assertTrue((root / "raw").exists())
            self.assertTrue(dataset.location.exists())

            rows = DuckDBQueryService(root / "artifacts" / "quantlab.duckdb").query_dataset(
                dataset,
                "select symbol, count(*) as rows, avg(price) as avg_price from dataset group by symbol",
            )
            self.assertEqual(rows[0]["rows"], 2)
            self.assertGreater(rows[0]["avg_price"], 100.0)

    def test_history_ensurer_downloads_missing_days_and_skips_existing(self) -> None:
        def add_zip_response(responses: dict[str, bytes | str], spec: BinanceHistoricalSpec, body: str) -> None:
            zip_bytes = build_zip_bytes("payload.csv", body)
            checksum = hashlib.sha256(zip_bytes).hexdigest()
            client = BinanceVisionClient(session=FakeSession({}))
            responses[client.remote_url(spec)] = zip_bytes
            checksum_spec = BinanceHistoricalSpec(
                market=spec.market,
                frequency=spec.frequency,
                dataset=spec.dataset,
                symbol=spec.symbol,
                date=spec.date,
                interval=spec.interval,
                with_checksum=True,
            )
            responses[client.remote_url(checksum_spec)] = f"{checksum}  payload.zip"

        responses: dict[str, bytes | str] = {}
        for version in ("2023-01-01", "2023-01-02"):
            add_zip_response(
                responses,
                BinanceHistoricalSpec(
                    market=BinanceMarket.SPOT,
                    frequency=BinanceFrequency.DAILY,
                    dataset=BinanceDataset.KLINES,
                    symbol="BTCUSDT",
                    date=version,
                    interval="1m",
                ),
                "1672531200000,100,110,90,105,10,1672617599999,1000,5,4,400,0\n",
            )
            add_zip_response(
                responses,
                BinanceHistoricalSpec(
                    market=BinanceMarket.SPOT,
                    frequency=BinanceFrequency.DAILY,
                    dataset=BinanceDataset.AGGTRADES,
                    symbol="BTCUSDT",
                    date=version,
                ),
                "1,100.1,0.5,1,1,1672531200000,false,true\n2,100.2,0.25,2,2,1672531201000,true,true\n",
            )

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ensurer = BinanceHistoryEnsurer(
                raw_store=RawArtifactStore(root / "raw"),
                dataset_store=ParquetMarketDataStore(),
                catalog=DataCatalog(root / "catalog" / "catalog.json"),
                client=BinanceVisionClient(session=FakeSession(responses)),
            )
            first = ensurer.ensure_range(
                symbols=["BTCUSDT"],
                start_date="2023-01-01",
                end_date="2023-01-02",
                normalized_base_path=root / "warehouse",
                interval="1m",
                datasets=(BinanceDataset.KLINES, BinanceDataset.AGGTRADES),
            )
            second = ensurer.ensure_range(
                symbols=["BTCUSDT"],
                start_date="2023-01-01",
                end_date="2023-01-02",
                normalized_base_path=root / "warehouse",
                interval="1m",
                datasets=(BinanceDataset.KLINES, BinanceDataset.AGGTRADES),
            )

            self.assertEqual(len(first.imported), 4)
            self.assertFalse(first.unavailable)
            self.assertEqual(len(second.imported), 0)
            self.assertEqual(len(second.existing), 4)


if __name__ == "__main__":
    unittest.main()
