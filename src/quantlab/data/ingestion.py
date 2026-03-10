from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol, Sequence

from quantlab.core.enums import DataFrequency, DatasetKind
from quantlab.core.interfaces import MarketDataSource, OrderBookDataSource, QuoteDataSource, TradeDataSource
from quantlab.core.models import Bar, Instrument, OrderBookSnapshot, Quote, Trade
from quantlab.data.catalog import DataCatalog, DatasetRef
from quantlab.data.schemas import schema_for_kind
from quantlab.data.transforms import BarTransformPipeline


@dataclass(frozen=True, slots=True)
class IngestionRequest:
    dataset_name: str
    version: str
    instrument: Instrument
    start: datetime
    end: datetime
    frequency: DataFrequency = DataFrequency.TICK
    storage_path: Path = Path("data/warehouse")
    partition_columns: tuple[str, ...] = ("symbol", "date")


class BarDatasetStore(Protocol):
    def write_bars(self, dataset: DatasetRef, bars: Sequence[Bar]) -> None:
        ...


class TradeDatasetStore(Protocol):
    def write_trades(self, dataset: DatasetRef, trades: Sequence[Trade]) -> None:
        ...


class QuoteDatasetStore(Protocol):
    def write_quotes(self, dataset: DatasetRef, quotes: Sequence[Quote]) -> None:
        ...


class OrderBookDatasetStore(Protocol):
    def write_order_book_snapshots(self, dataset: DatasetRef, snapshots: Sequence[OrderBookSnapshot]) -> None:
        ...


class MarketDataIngestionService:
    def __init__(
        self,
        source: MarketDataSource,
        store: BarDatasetStore,
        catalog: DataCatalog,
        transforms: BarTransformPipeline | None = None,
    ) -> None:
        self._source = source
        self._store = store
        self._catalog = catalog
        self._transforms = transforms or BarTransformPipeline()

    def ingest(self, request: IngestionRequest) -> DatasetRef:
        bars = self._source.fetch_bars(
            instrument=request.instrument,
            start=request.start,
            end=request.end,
            frequency=request.frequency,
        )
        transformed_bars = self._transforms.run(bars)
        dataset = build_dataset_ref(
            request=request,
            data_kind=DatasetKind.BAR,
            row_count=len(transformed_bars),
        )
        self._store.write_bars(dataset, transformed_bars)
        self._catalog.register(dataset)
        return dataset


class TradeIngestionService:
    def __init__(self, source: TradeDataSource, store: TradeDatasetStore, catalog: DataCatalog) -> None:
        self._source = source
        self._store = store
        self._catalog = catalog

    def ingest(self, request: IngestionRequest) -> DatasetRef:
        trades = self._source.fetch_trades(request.instrument, request.start, request.end)
        dataset = build_dataset_ref(request=request, data_kind=DatasetKind.TRADE, row_count=len(trades))
        self._store.write_trades(dataset, trades)
        self._catalog.register(dataset)
        return dataset


class QuoteIngestionService:
    def __init__(self, source: QuoteDataSource, store: QuoteDatasetStore, catalog: DataCatalog) -> None:
        self._source = source
        self._store = store
        self._catalog = catalog

    def ingest(self, request: IngestionRequest) -> DatasetRef:
        quotes = self._source.fetch_quotes(request.instrument, request.start, request.end, request.frequency)
        dataset = build_dataset_ref(request=request, data_kind=DatasetKind.QUOTE, row_count=len(quotes))
        self._store.write_quotes(dataset, quotes)
        self._catalog.register(dataset)
        return dataset


class OrderBookSnapshotIngestionService:
    def __init__(self, source: OrderBookDataSource, store: OrderBookDatasetStore, catalog: DataCatalog) -> None:
        self._source = source
        self._store = store
        self._catalog = catalog

    def ingest(self, request: IngestionRequest) -> DatasetRef:
        snapshots = self._source.fetch_order_book_snapshots(
            request.instrument,
            request.start,
            request.end,
            request.frequency,
        )
        row_count = sum(len(snapshot.bids) + len(snapshot.asks) for snapshot in snapshots)
        dataset = build_dataset_ref(
            request=request,
            data_kind=DatasetKind.ORDER_BOOK_SNAPSHOT,
            row_count=row_count,
        )
        self._store.write_order_book_snapshots(dataset, snapshots)
        self._catalog.register(dataset)
        return dataset


def build_dataset_ref(request: IngestionRequest, data_kind: DatasetKind, row_count: int) -> DatasetRef:
    schema = schema_for_kind(data_kind)
    return DatasetRef(
        name=request.dataset_name,
        version=request.version,
        data_kind=data_kind,
        asset_class=request.instrument.asset_class,
        location=request.storage_path / request.dataset_name / request.version,
        schema=schema.columns,
        row_count=row_count,
        partition_columns=request.partition_columns,
        metadata={
            "frequency": request.frequency.value,
            "instrument": request.instrument.symbol,
            "dataset_kind": data_kind.value,
        },
    )
