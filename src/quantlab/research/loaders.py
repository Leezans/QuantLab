from __future__ import annotations

from collections.abc import Sequence

from quantlab.core.enums import DatasetKind
from quantlab.core.models import Bar, OrderBookSnapshot, Trade
from quantlab.data.catalog import DataCatalog, DatasetRef
from quantlab.data.stores import ParquetMarketDataStore


class HistoricalBarLoader:
    def __init__(self, catalog: DataCatalog, store: ParquetMarketDataStore) -> None:
        self._catalog = catalog
        self._store = store

    def load_latest(self, dataset_names: Sequence[str]) -> tuple[Bar, ...]:
        datasets = [self._catalog.resolve(dataset_name, version="latest") for dataset_name in dataset_names]
        return self._read_datasets(datasets)

    def load_range(
        self,
        dataset_names: Sequence[str],
        *,
        start_version: str | None = None,
        end_version: str | None = None,
    ) -> tuple[Bar, ...]:
        requested_names = set(dataset_names)
        datasets = [
            dataset
            for dataset in self._catalog.list()
            if dataset.name in requested_names
            and dataset.data_kind is DatasetKind.BAR
            and (start_version is None or dataset.version >= start_version)
            and (end_version is None or dataset.version <= end_version)
        ]
        return self._read_datasets(datasets)

    def load_binance_klines(
        self,
        symbols: Sequence[str],
        *,
        interval: str = "1d",
        market: str = "spot",
        start_version: str | None = None,
        end_version: str | None = None,
    ) -> tuple[Bar, ...]:
        dataset_names: list[str] = []
        for symbol in symbols:
            dataset_names.append(f"binance.{market}.klines.{symbol.lower()}.{interval.lower()}")
            dataset_names.append(f"binance.{market}.klines.{symbol.lower()}")
        return self.load_range(dataset_names, start_version=start_version, end_version=end_version)

    def _read_datasets(self, datasets: Sequence[DatasetRef]) -> tuple[Bar, ...]:
        bars: list[Bar] = []
        for dataset in sorted(datasets, key=lambda item: (item.version, item.name)):
            bars.extend(self._store.read_bars(dataset))
        return tuple(sorted(bars, key=lambda bar: (bar.timestamp, bar.instrument.symbol)))


class HistoricalTradeLoader:
    def __init__(self, catalog: DataCatalog, store: ParquetMarketDataStore) -> None:
        self._catalog = catalog
        self._store = store

    def load_range(
        self,
        dataset_names: Sequence[str],
        *,
        start_version: str | None = None,
        end_version: str | None = None,
    ) -> tuple[Trade, ...]:
        requested_names = set(dataset_names)
        datasets = [
            dataset
            for dataset in self._catalog.list()
            if dataset.name in requested_names
            and dataset.data_kind is DatasetKind.TRADE
            and (start_version is None or dataset.version >= start_version)
            and (end_version is None or dataset.version <= end_version)
        ]
        trades: list[Trade] = []
        for dataset in sorted(datasets, key=lambda item: (item.version, item.name)):
            trades.extend(self._store.read_trades(dataset))
        return tuple(sorted(trades, key=lambda trade: (trade.timestamp, trade.instrument.symbol, trade.trade_id)))

    def load_binance_trades(
        self,
        symbols: Sequence[str],
        *,
        dataset: str = "aggTrades",
        market: str = "spot",
        start_version: str | None = None,
        end_version: str | None = None,
    ) -> tuple[Trade, ...]:
        dataset_names = [f"binance.{market}.{dataset}.{symbol.lower()}" for symbol in symbols]
        return self.load_range(dataset_names, start_version=start_version, end_version=end_version)


class HistoricalOrderBookLoader:
    def __init__(self, catalog: DataCatalog, store: ParquetMarketDataStore) -> None:
        self._catalog = catalog
        self._store = store

    def load_range(
        self,
        dataset_names: Sequence[str],
        *,
        start_version: str | None = None,
        end_version: str | None = None,
    ) -> tuple[OrderBookSnapshot, ...]:
        requested_names = set(dataset_names)
        datasets = [
            dataset
            for dataset in self._catalog.list()
            if dataset.name in requested_names
            and dataset.data_kind is DatasetKind.ORDER_BOOK_SNAPSHOT
            and (start_version is None or dataset.version >= start_version)
            and (end_version is None or dataset.version <= end_version)
        ]
        snapshots: list[OrderBookSnapshot] = []
        for dataset in sorted(datasets, key=lambda item: (item.version, item.name)):
            snapshots.extend(self._store.read_order_book_snapshots(dataset))
        return tuple(sorted(snapshots, key=lambda snapshot: (snapshot.timestamp, snapshot.instrument.symbol, snapshot.sequence_id)))

    def load_binance_synthetic_depth(
        self,
        symbols: Sequence[str],
        *,
        interval: str = "1m",
        market: str = "spot",
        start_version: str | None = None,
        end_version: str | None = None,
    ) -> tuple[OrderBookSnapshot, ...]:
        dataset_names = [f"binance.{market}.synthetic_depth.{symbol.lower()}.{interval.lower()}" for symbol in symbols]
        return self.load_range(dataset_names, start_version=start_version, end_version=end_version)
