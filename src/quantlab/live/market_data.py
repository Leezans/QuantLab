from __future__ import annotations

import shutil
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantlab.core.enums import DataFrequency, DatasetKind, StorageTier
from quantlab.core.models import Bar, Instrument, OrderBookSnapshot, Quote, Trade
from quantlab.data import (
    BinanceRESTMarketDataClient,
    DataCatalog,
    DatasetRef,
    IngestionRequest,
    ParquetMarketDataStore,
    build_dataset_ref,
)
from quantlab.live.feeds import BinanceRealtimeChannel, BinanceRealtimeNormalizer, BinanceStreamSubscription


MarketEvent = Bar | Trade | Quote | OrderBookSnapshot


@dataclass(frozen=True, slots=True)
class GapFillBatch:
    subscription: BinanceStreamSubscription
    events: tuple[MarketEvent, ...]
    datasets: tuple[DatasetRef, ...]


@dataclass(frozen=True, slots=True)
class CacheReconciliationResult:
    version: str
    purged_trade_caches: tuple[DatasetRef, ...]
    archived_cache_datasets: tuple[DatasetRef, ...]
    retained_cache_datasets: tuple[DatasetRef, ...]


class IntradayEventPersistenceService:
    def __init__(
        self,
        store: ParquetMarketDataStore,
        catalog: DataCatalog,
        base_path: Path,
        *,
        dataset_namespace: str = "binance.spot.cache",
    ) -> None:
        self._store = store
        self._catalog = catalog
        self._base_path = Path(base_path)
        self._dataset_namespace = dataset_namespace

    def persist_event(self, event: MarketEvent, *, source: str) -> DatasetRef:
        return self.persist_events((event,), source=source)[0]

    def persist_events(self, events: Sequence[MarketEvent], *, source: str) -> tuple[DatasetRef, ...]:
        grouped: dict[tuple[str, str], list[MarketEvent]] = defaultdict(list)
        for event in events:
            grouped[(self._dataset_name(event), self._dataset_version(event))].append(event)

        persisted: list[DatasetRef] = []
        for (dataset_name, version), grouped_events in grouped.items():
            existing = self._catalog.get(dataset_name, version)
            if existing is None:
                dataset = self._build_dataset(dataset_name, version, grouped_events, source=source)
                row_count = self._write_dataset(dataset, grouped_events, append=False)
                dataset = replace(dataset, row_count=row_count)
            else:
                row_count = self._write_dataset(existing, grouped_events, append=True)
                dataset = replace(
                    existing,
                    row_count=row_count,
                    metadata={
                        **existing.metadata,
                        "last_source": source,
                        "last_updated_at": datetime.now(UTC).isoformat(),
                    },
                )
            self._catalog.register(dataset)
            persisted.append(dataset)
        return tuple(sorted(persisted, key=lambda dataset: (dataset.name, dataset.version)))

    def _build_dataset(
        self,
        dataset_name: str,
        version: str,
        events: Sequence[MarketEvent],
        *,
        source: str,
    ) -> DatasetRef:
        sample = events[0]
        instrument = sample.instrument
        timestamps = [event.timestamp for event in events]
        request = IngestionRequest(
            dataset_name=dataset_name,
            version=version,
            instrument=instrument,
            start=min(timestamps),
            end=max(timestamps),
            frequency=self._frequency_for_events(events),
            storage_path=self._base_path,
        )
        dataset = build_dataset_ref(
            request=request,
            data_kind=self._dataset_kind(sample),
            row_count=self._row_count(events),
        )
        interval = self._bar_interval(sample) if isinstance(sample, Bar) else ""
        return replace(
            dataset,
            storage_tier=StorageTier.CACHE,
            metadata={
                **dataset.metadata,
                "source": source,
                "mode": "intraday",
                "venue": instrument.venue,
                "symbol": instrument.symbol,
                "interval": interval,
                "lifecycle_policy": self._lifecycle_policy(sample),
            },
        )

    def read_events(self, dataset: DatasetRef) -> tuple[MarketEvent, ...]:
        if dataset.data_kind is DatasetKind.BAR:
            return self._store.read_bars(dataset)
        if dataset.data_kind is DatasetKind.TRADE:
            return self._store.read_trades(dataset)
        if dataset.data_kind is DatasetKind.QUOTE:
            return self._store.read_quotes(dataset)
        return self._store.read_order_book_snapshots(dataset)

    def _write_dataset(self, dataset: DatasetRef, events: Sequence[MarketEvent], *, append: bool) -> int:
        sample = events[0]
        if isinstance(sample, Bar):
            payload = tuple(event for event in events if isinstance(event, Bar))
            return self._store.append_bars(dataset, payload) if append else self._write_bars(dataset, payload)
        if isinstance(sample, Trade):
            payload = tuple(event for event in events if isinstance(event, Trade))
            return self._store.append_trades(dataset, payload) if append else self._write_trades(dataset, payload)
        if isinstance(sample, Quote):
            payload = tuple(event for event in events if isinstance(event, Quote))
            return self._store.append_quotes(dataset, payload) if append else self._write_quotes(dataset, payload)
        payload = tuple(event for event in events if isinstance(event, OrderBookSnapshot))
        return (
            self._store.append_order_book_snapshots(dataset, payload)
            if append
            else self._write_order_book_snapshots(dataset, payload)
        )

    def _write_bars(self, dataset: DatasetRef, bars: Sequence[Bar]) -> int:
        self._store.write_bars(dataset, bars)
        return len(bars)

    def _write_trades(self, dataset: DatasetRef, trades: Sequence[Trade]) -> int:
        self._store.write_trades(dataset, trades)
        return len(trades)

    def _write_quotes(self, dataset: DatasetRef, quotes: Sequence[Quote]) -> int:
        self._store.write_quotes(dataset, quotes)
        return len(quotes)

    def _write_order_book_snapshots(self, dataset: DatasetRef, snapshots: Sequence[OrderBookSnapshot]) -> int:
        self._store.write_order_book_snapshots(dataset, snapshots)
        return sum(len(snapshot.bids) + len(snapshot.asks) for snapshot in snapshots)

    def _dataset_name(self, event: MarketEvent) -> str:
        symbol = event.instrument.symbol.lower()
        if isinstance(event, Bar):
            interval = self._bar_interval(event)
            return f"{self._dataset_namespace}.klines.{symbol}.{interval}"
        if isinstance(event, Trade):
            return f"{self._dataset_namespace}.trades.{symbol}"
        if isinstance(event, Quote):
            return f"{self._dataset_namespace}.quotes.{symbol}"
        return f"{self._dataset_namespace}.order_book.{symbol}"

    def _dataset_version(self, event: MarketEvent) -> str:
        return event.timestamp.astimezone(UTC).date().isoformat()

    def _dataset_kind(self, event: MarketEvent) -> DatasetKind:
        if isinstance(event, Bar):
            return DatasetKind.BAR
        if isinstance(event, Trade):
            return DatasetKind.TRADE
        if isinstance(event, Quote):
            return DatasetKind.QUOTE
        return DatasetKind.ORDER_BOOK_SNAPSHOT

    def _frequency_for_events(self, events: Sequence[MarketEvent]) -> DataFrequency:
        sample = events[0]
        if isinstance(sample, Bar):
            interval = self._bar_interval(sample)
            return {
                "1m": DataFrequency.ONE_MINUTE,
                "5m": DataFrequency.FIVE_MINUTE,
                "1h": DataFrequency.HOURLY,
                "1d": DataFrequency.DAILY,
            }.get(interval, DataFrequency.TICK)
        return DataFrequency.TICK

    def _bar_interval(self, bar: Bar) -> str:
        return str(bar.metadata.get("interval", "realtime"))

    def _row_count(self, events: Sequence[MarketEvent]) -> int:
        sample = events[0]
        if isinstance(sample, OrderBookSnapshot):
            return sum(len(snapshot.bids) + len(snapshot.asks) for snapshot in events if isinstance(snapshot, OrderBookSnapshot))
        return len(events)

    def _lifecycle_policy(self, event: MarketEvent) -> str:
        if isinstance(event, Trade):
            return "purge_when_historical_available"
        return "archive_to_history"


class IntradayCacheReconciliationService:
    def __init__(
        self,
        store: ParquetMarketDataStore,
        catalog: DataCatalog,
        cache_persistence: IntradayEventPersistenceService,
        historical_base_path: Path,
        *,
        archive_namespace: str = "binance.spot.archive",
    ) -> None:
        self._store = store
        self._catalog = catalog
        self._cache_persistence = cache_persistence
        self._historical_base_path = Path(historical_base_path)
        self._archive_namespace = archive_namespace

    def reconcile(self, version: str, symbol: str | None = None) -> CacheReconciliationResult:
        normalized_symbol = symbol.upper() if symbol else None
        purged: list[DatasetRef] = []
        archived: list[DatasetRef] = []
        retained: list[DatasetRef] = []

        for dataset in self._cache_datasets(version=version, symbol=normalized_symbol):
            if dataset.data_kind is DatasetKind.TRADE and self._has_historical_trade(dataset):
                purged.append(dataset)
                self._delete_cache_dataset(dataset)
                continue
            if dataset.data_kind is DatasetKind.TRADE:
                retained.append(dataset)
                continue

            archived_dataset = self._archive_dataset(dataset)
            archived.append(archived_dataset)
            self._delete_cache_dataset(dataset)

        return CacheReconciliationResult(
            version=version,
            purged_trade_caches=tuple(sorted(purged, key=lambda dataset: dataset.name)),
            archived_cache_datasets=tuple(sorted(archived, key=lambda dataset: dataset.name)),
            retained_cache_datasets=tuple(sorted(retained, key=lambda dataset: dataset.name)),
        )

    def _cache_datasets(self, *, version: str, symbol: str | None) -> tuple[DatasetRef, ...]:
        datasets: list[DatasetRef] = []
        for dataset in self._catalog.list():
            if dataset.version != version:
                continue
            if dataset.storage_tier is not StorageTier.CACHE:
                continue
            if symbol and str(dataset.metadata.get("symbol", "")).upper() != symbol:
                continue
            datasets.append(dataset)
        return tuple(datasets)

    def _has_historical_trade(self, cache_dataset: DatasetRef) -> bool:
        symbol = str(cache_dataset.metadata.get("symbol", "")).lower()
        candidate_names = (
            f"binance.spot.aggTrades.{symbol}",
            f"binance.spot.trades.{symbol}",
        )
        return any(self._catalog.get(name, cache_dataset.version) is not None for name in candidate_names)

    def _archive_dataset(self, cache_dataset: DatasetRef) -> DatasetRef:
        events = self._cache_persistence.read_events(cache_dataset)
        archive_name = self._archive_dataset_name(cache_dataset)
        existing = self._catalog.get(archive_name, cache_dataset.version)
        if existing is None:
            archive_dataset = replace(
                cache_dataset,
                name=archive_name,
                location=self._historical_base_path / archive_name / cache_dataset.version,
                storage_tier=StorageTier.NORMALIZED,
                metadata={
                    **cache_dataset.metadata,
                    "source": "intraday_cache_archive",
                    "archived_from": str(cache_dataset.location),
                },
            )
            row_count = self._write_events(archive_dataset, events, append=False)
        else:
            row_count = self._write_events(existing, events, append=True)
            archive_dataset = replace(
                existing,
                row_count=row_count,
                metadata={
                    **existing.metadata,
                    "source": "intraday_cache_archive",
                    "archived_from": str(cache_dataset.location),
                    "last_archived_at": datetime.now(UTC).isoformat(),
                },
            )
        self._catalog.register(archive_dataset)
        return archive_dataset

    def _archive_dataset_name(self, cache_dataset: DatasetRef) -> str:
        symbol = str(cache_dataset.metadata.get("symbol", "")).lower()
        interval = str(cache_dataset.metadata.get("interval", "")).strip(".")
        if cache_dataset.data_kind is DatasetKind.BAR:
            return f"{self._archive_namespace}.klines.{symbol}.{interval or 'realtime'}"
        if cache_dataset.data_kind is DatasetKind.QUOTE:
            return f"{self._archive_namespace}.quotes.{symbol}"
        if cache_dataset.data_kind is DatasetKind.ORDER_BOOK_SNAPSHOT:
            return f"{self._archive_namespace}.order_book.{symbol}"
        return f"{self._archive_namespace}.trades.{symbol}"

    def _write_events(self, dataset: DatasetRef, events: Sequence[MarketEvent], *, append: bool) -> int:
        sample = events[0]
        if isinstance(sample, Bar):
            payload = tuple(event for event in events if isinstance(event, Bar))
            return self._store.append_bars(dataset, payload) if append else self._write_bars(dataset, payload)
        if isinstance(sample, Quote):
            payload = tuple(event for event in events if isinstance(event, Quote))
            return self._store.append_quotes(dataset, payload) if append else self._write_quotes(dataset, payload)
        if isinstance(sample, OrderBookSnapshot):
            payload = tuple(event for event in events if isinstance(event, OrderBookSnapshot))
            return (
                self._store.append_order_book_snapshots(dataset, payload)
                if append
                else self._write_order_book_snapshots(dataset, payload)
            )
        payload = tuple(event for event in events if isinstance(event, Trade))
        return self._store.append_trades(dataset, payload) if append else self._write_trades(dataset, payload)

    def _write_bars(self, dataset: DatasetRef, bars: Sequence[Bar]) -> int:
        self._store.write_bars(dataset, bars)
        return len(bars)

    def _write_quotes(self, dataset: DatasetRef, quotes: Sequence[Quote]) -> int:
        self._store.write_quotes(dataset, quotes)
        return len(quotes)

    def _write_order_book_snapshots(self, dataset: DatasetRef, snapshots: Sequence[OrderBookSnapshot]) -> int:
        self._store.write_order_book_snapshots(dataset, snapshots)
        return sum(len(snapshot.bids) + len(snapshot.asks) for snapshot in snapshots)

    def _write_trades(self, dataset: DatasetRef, trades: Sequence[Trade]) -> int:
        self._store.write_trades(dataset, trades)
        return len(trades)

    def _delete_cache_dataset(self, dataset: DatasetRef) -> None:
        shutil.rmtree(dataset.location, ignore_errors=True)
        self._catalog.unregister(dataset.name, dataset.version)


class BinanceGapFillService:
    def __init__(
        self,
        rest_client: BinanceRESTMarketDataClient,
        persistence: IntradayEventPersistenceService,
    ) -> None:
        self._rest_client = rest_client
        self._persistence = persistence

    def backfill_subscriptions(
        self,
        subscriptions: Sequence[BinanceStreamSubscription],
        start: datetime,
        end: datetime,
    ) -> tuple[GapFillBatch, ...]:
        batches: list[GapFillBatch] = []
        for subscription in subscriptions:
            events = self._fetch_subscription_events(subscription, start, end)
            datasets = self._persistence.persist_events(events, source="binance_rest_gap_fill") if events else ()
            batches.append(GapFillBatch(subscription=subscription, events=events, datasets=datasets))
        return tuple(batches)

    def _fetch_subscription_events(
        self,
        subscription: BinanceStreamSubscription,
        start: datetime,
        end: datetime,
    ) -> tuple[MarketEvent, ...]:
        if subscription.channel is BinanceRealtimeChannel.AGG_TRADE:
            return self._rest_client.fetch_agg_trades(subscription.symbol, start=start, end=end)
        if subscription.channel is BinanceRealtimeChannel.KLINE:
            interval = subscription.interval or "1m"
            return self._rest_client.fetch_klines(subscription.symbol, interval=interval, start=start, end=end)
        if subscription.channel is BinanceRealtimeChannel.BOOK_TICKER:
            return (self._rest_client.fetch_book_ticker(subscription.symbol, as_of=end),)
        if subscription.channel in {BinanceRealtimeChannel.DEPTH, BinanceRealtimeChannel.PARTIAL_DEPTH}:
            depth_limit = subscription.depth or 20
            return (self._rest_client.fetch_order_book_snapshot(subscription.symbol, limit=depth_limit, as_of=end),)
        if subscription.channel is BinanceRealtimeChannel.TRADE:
            raise ValueError("raw trade gap fill is not supported in the bootstrap without authenticated historicalTrades access")
        raise ValueError(f"unsupported subscription channel: {subscription.channel}")


class BinanceStitchedMarketDataService:
    def __init__(
        self,
        gap_fill: BinanceGapFillService,
        persistence: IntradayEventPersistenceService,
        normalizer: BinanceRealtimeNormalizer | None = None,
    ) -> None:
        self._gap_fill = gap_fill
        self._persistence = persistence
        self._normalizer = normalizer or BinanceRealtimeNormalizer()
        self._watermarks: dict[tuple[str, str], tuple[Any, ...]] = {}

    def prime(
        self,
        subscriptions: Sequence[BinanceStreamSubscription],
        start: datetime,
        end: datetime,
    ) -> tuple[DatasetRef, ...]:
        batches = self._gap_fill.backfill_subscriptions(subscriptions, start=start, end=end)
        datasets: list[DatasetRef] = []
        for batch in batches:
            if batch.events:
                self._update_watermark(batch.events[-1])
            datasets.extend(batch.datasets)
        return tuple(sorted(datasets, key=lambda dataset: (dataset.name, dataset.version)))

    def consume_payload(self, payload: Mapping[str, Any], *, source: str = "binance_ws") -> DatasetRef | None:
        return self.consume_event(self._normalizer.normalize_payload(payload), source=source)

    def consume_event(self, event: MarketEvent, *, source: str = "binance_ws") -> DatasetRef | None:
        if self._is_stale(event):
            return None
        dataset = self._persistence.persist_event(event, source=source)
        self._update_watermark(event)
        return dataset

    def _is_stale(self, event: MarketEvent) -> bool:
        key = self._watermark_key(event)
        watermark = self._watermarks.get(key)
        if watermark is None:
            return False
        return self._watermark_value(event) <= watermark

    def _update_watermark(self, event: MarketEvent) -> None:
        key = self._watermark_key(event)
        value = self._watermark_value(event)
        current = self._watermarks.get(key)
        if current is None or value > current:
            self._watermarks[key] = value

    def _watermark_key(self, event: MarketEvent) -> tuple[str, str]:
        if isinstance(event, Bar):
            return ("bar", f"{event.instrument.symbol}:{event.metadata.get('interval', 'realtime')}")
        if isinstance(event, Trade):
            return ("trade", event.instrument.symbol)
        if isinstance(event, Quote):
            return ("quote", event.instrument.symbol)
        return ("order_book_snapshot", event.instrument.symbol)

    def _watermark_value(self, event: MarketEvent) -> tuple[Any, ...]:
        if isinstance(event, Bar):
            return (
                event.timestamp,
                int(str(event.metadata.get("event_time", event.metadata.get("close_time", "0")) or "0")),
            )
        if isinstance(event, Trade):
            return (event.timestamp, _safe_int(event.trade_id))
        if isinstance(event, Quote):
            return (event.timestamp, _safe_int(event.metadata.get("sequence_id", "0")))
        return (event.timestamp, _safe_int(event.sequence_id))


def _safe_int(value: Any) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0
