from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

from quantlab.core.enums import AssetClass, DatasetKind
from quantlab.data.catalog import DataCatalog, DatasetRef

FASTAPI_AVAILABLE = importlib.util.find_spec("fastapi") is not None

if FASTAPI_AVAILABLE:
    from fastapi.testclient import TestClient
    from quantlab.api import create_app


@unittest.skipUnless(FASTAPI_AVAILABLE, "fastapi is not installed")
class FastApiIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.catalog_path = self.root / "catalog" / "catalog.json"
        self.dataset_dir = self.root / "warehouse" / "bars" / "v1"
        self.dataset_dir.mkdir(parents=True, exist_ok=True)

        pa_spec = importlib.util.find_spec("pyarrow")
        if pa_spec is None:
            self.skipTest("pyarrow is not installed")
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"timestamp": ["2024-01-01T00:00:00+00:00"], "close": [101.0]})
        pq.write_table(table, self.dataset_dir / "part-000.parquet")

        catalog = DataCatalog(self.catalog_path)
        catalog.register(
            DatasetRef(
                name="bars.demo",
                version="v1",
                data_kind=DatasetKind.BAR,
                asset_class=AssetClass.CRYPTO,
                location=self.dataset_dir,
                schema=("timestamp", "close"),
                row_count=1,
            )
        )

        self.config_path = self.root / "config.toml"
        self.config_path.write_text(
            "\n".join(
                [
                    "[project]",
                    'name = "QuantLab"',
                    'environment = "test"',
                    "[storage]",
                    f'raw_data_dir = "{(self.root / "raw").as_posix()}"',
                    f'curated_data_dir = "{(self.root / "curated").as_posix()}"',
                    f'feature_store_dir = "{(self.root / "features").as_posix()}"',
                    f'intraday_cache_dir = "{(self.root / "intraday_cache").as_posix()}"',
                    f'warehouse_dir = "{(self.root / "warehouse").as_posix()}"',
                    f'catalog_path = "{self.catalog_path.as_posix()}"',
                    f'duckdb_path = "{(self.root / "artifacts" / "quantlab.duckdb").as_posix()}"',
                    f'artifact_dir = "{(self.root / "artifacts").as_posix()}"',
                    "[research]",
                    'default_universe = ["BTCUSDT"]',
                    'primary_frequency = "1d"',
                    'base_currency = "USD"',
                    "[runtime]",
                    'timezone = "UTC"',
                    "max_workers = 2",
                    "[execution]",
                    "paper_trading = true",
                    "default_slippage_bps = 1.0",
                ]
            ),
            encoding="utf-8",
        )

        self.client = TestClient(create_app(config_path=str(self.config_path)))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_health_endpoint(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_dataset_query_endpoint(self) -> None:
        response = self.client.post(
            "/datasets/query",
            json={"name": "bars.demo", "version": "v1", "sql": "select close from dataset"},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["row_count"], 1)
        self.assertEqual(body["rows"][0]["close"], 101.0)


if __name__ == "__main__":
    unittest.main()
