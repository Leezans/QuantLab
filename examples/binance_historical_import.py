from __future__ import annotations

from quantlab.config.loader import load_settings
from quantlab.data import (
    BinanceDataset,
    BinanceFrequency,
    BinanceHistoricalImporter,
    BinanceHistoricalSpec,
    BinanceMarket,
    DataCatalog,
    ParquetMarketDataStore,
    RawArtifactStore,
)


def main() -> None:
    settings = load_settings("config/base.toml")
    importer = BinanceHistoricalImporter(
        raw_store=RawArtifactStore(settings.storage.raw_data_dir),
        dataset_store=ParquetMarketDataStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
    )
    dataset = importer.ingest(
        spec=BinanceHistoricalSpec(
            market=BinanceMarket.SPOT,
            frequency=BinanceFrequency.DAILY,
            dataset=BinanceDataset.KLINES,
            symbol="BTCUSDT",
            date="2023-01-01",
            interval="1d",
        ),
        normalized_base_path=settings.storage.warehouse_dir,
    )
    print(dataset)


if __name__ == "__main__":
    main()
