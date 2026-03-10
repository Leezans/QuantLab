from __future__ import annotations

import json
import shutil
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from quantlab.core.enums import AssetClass, DatasetKind, Side
from quantlab.core.models import Bar, Instrument, OrderBookLevel, OrderBookSnapshot, Quote, Trade
from quantlab.data.catalog import DatasetRef


class InMemoryBarStore:
    """Bootstrap store for research workflows before durable storage is introduced."""

    def __init__(self) -> None:
        self._bars: dict[tuple[str, str], list[Bar]] = defaultdict(list)

    def write_bars(self, dataset: DatasetRef, bars: Sequence[Bar]) -> None:
        key = (dataset.name, dataset.version)
        self._bars[key] = list(bars)

    def read_bars(self, dataset: DatasetRef) -> tuple[Bar, ...]:
        key = (dataset.name, dataset.version)
        return tuple(self._bars.get(key, []))


class ParquetMarketDataStore:
    """Partitioned Parquet store for normalized historical market datasets."""

    def write_bars(self, dataset: DatasetRef, bars: Sequence[Bar]) -> None:
        self._assert_kind(dataset, DatasetKind.BAR)
        self._write_rows(dataset, [_bar_to_record(bar) for bar in bars])

    def read_bars(self, dataset: DatasetRef) -> tuple[Bar, ...]:
        self._assert_kind(dataset, DatasetKind.BAR)
        return tuple(
            sorted(
                (_record_to_bar(record) for record in self._read_rows(dataset)),
                key=lambda bar: (bar.timestamp, bar.instrument.symbol),
            )
        )

    def append_bars(self, dataset: DatasetRef, bars: Sequence[Bar]) -> int:
        self._assert_kind(dataset, DatasetKind.BAR)
        merged = _merge_by_key(
            self.read_bars(dataset),
            bars,
            key=lambda bar: (bar.instrument.symbol, bar.timestamp),
            sort_key=lambda bar: (bar.timestamp, bar.instrument.symbol),
        )
        self.write_bars(dataset, merged)
        return len(merged)

    def write_trades(self, dataset: DatasetRef, trades: Sequence[Trade]) -> None:
        self._assert_kind(dataset, DatasetKind.TRADE)
        self._write_rows(dataset, [_trade_to_record(trade) for trade in trades])

    def read_trades(self, dataset: DatasetRef) -> tuple[Trade, ...]:
        self._assert_kind(dataset, DatasetKind.TRADE)
        return tuple(
            sorted(
                (_record_to_trade(record) for record in self._read_rows(dataset)),
                key=lambda trade: (trade.timestamp, trade.trade_id),
            )
        )

    def append_trades(self, dataset: DatasetRef, trades: Sequence[Trade]) -> int:
        self._assert_kind(dataset, DatasetKind.TRADE)
        merged = _merge_by_key(
            self.read_trades(dataset),
            trades,
            key=lambda trade: (trade.instrument.symbol, trade.timestamp, trade.trade_id),
            sort_key=lambda trade: (trade.timestamp, trade.trade_id),
        )
        self.write_trades(dataset, merged)
        return len(merged)

    def write_quotes(self, dataset: DatasetRef, quotes: Sequence[Quote]) -> None:
        self._assert_kind(dataset, DatasetKind.QUOTE)
        self._write_rows(dataset, [_quote_to_record(quote) for quote in quotes])

    def read_quotes(self, dataset: DatasetRef) -> tuple[Quote, ...]:
        self._assert_kind(dataset, DatasetKind.QUOTE)
        return tuple(sorted((_record_to_quote(record) for record in self._read_rows(dataset)), key=lambda quote: quote.timestamp))

    def append_quotes(self, dataset: DatasetRef, quotes: Sequence[Quote]) -> int:
        self._assert_kind(dataset, DatasetKind.QUOTE)
        merged = _merge_by_key(
            self.read_quotes(dataset),
            quotes,
            key=lambda quote: (
                quote.instrument.symbol,
                quote.timestamp,
                str(quote.metadata.get("sequence_id", "")),
            ),
            sort_key=lambda quote: (quote.timestamp, str(quote.metadata.get("sequence_id", ""))),
        )
        self.write_quotes(dataset, merged)
        return len(merged)

    def write_order_book_snapshots(self, dataset: DatasetRef, snapshots: Sequence[OrderBookSnapshot]) -> None:
        self._assert_kind(dataset, DatasetKind.ORDER_BOOK_SNAPSHOT)
        rows: list[dict[str, Any]] = []
        for snapshot in snapshots:
            rows.extend(_order_book_snapshot_to_records(snapshot))
        self._write_rows(dataset, rows)

    def read_order_book_snapshots(self, dataset: DatasetRef) -> tuple[OrderBookSnapshot, ...]:
        self._assert_kind(dataset, DatasetKind.ORDER_BOOK_SNAPSHOT)
        return _records_to_order_book_snapshots(self._read_rows(dataset))

    def append_order_book_snapshots(self, dataset: DatasetRef, snapshots: Sequence[OrderBookSnapshot]) -> int:
        self._assert_kind(dataset, DatasetKind.ORDER_BOOK_SNAPSHOT)
        merged = _merge_by_key(
            self.read_order_book_snapshots(dataset),
            snapshots,
            key=lambda snapshot: (snapshot.instrument.symbol, snapshot.timestamp, snapshot.sequence_id),
            sort_key=lambda snapshot: (snapshot.timestamp, snapshot.sequence_id),
        )
        self.write_order_book_snapshots(dataset, merged)
        return sum(len(snapshot.bids) + len(snapshot.asks) for snapshot in merged)

    def _write_rows(self, dataset: DatasetRef, rows: Sequence[dict[str, Any]]) -> None:
        if dataset.format != "parquet":
            raise ValueError(f"unsupported dataset format: {dataset.format}")

        pa, ds = _require_pyarrow()
        dataset.location.mkdir(parents=True, exist_ok=True)
        if any(dataset.location.iterdir()):
            shutil.rmtree(dataset.location)
            dataset.location.mkdir(parents=True, exist_ok=True)
        if not rows:
            return
        table = pa.Table.from_pylist(list(rows))
        ds.write_dataset(
            table,
            base_dir=str(dataset.location),
            format="parquet",
            partitioning=list(dataset.partition_columns) or ["symbol", "date"],
            partitioning_flavor="hive",
            existing_data_behavior="overwrite_or_ignore",
        )

    def _read_rows(self, dataset: DatasetRef) -> tuple[dict[str, Any], ...]:
        _, ds = _require_pyarrow()
        if not any(dataset.location.rglob("*.parquet")):
            return ()
        scanner = ds.dataset(str(dataset.location), format="parquet", partitioning="hive")
        return tuple(scanner.to_table().to_pylist())

    def _assert_kind(self, dataset: DatasetRef, expected: DatasetKind) -> None:
        if dataset.data_kind is not expected:
            raise ValueError(f"dataset {dataset.name}:{dataset.version} has kind {dataset.data_kind}, expected {expected}")


class ParquetBarStore(ParquetMarketDataStore):
    """Backward-compatible bar-focused alias for older workflow code."""


def _base_record(timestamp: Any, instrument: Instrument) -> dict[str, Any]:
    return {
        "timestamp": timestamp,
        "date": timestamp.date().isoformat(),
        "symbol": instrument.symbol,
        "venue": instrument.venue,
        "asset_class": instrument.asset_class.value,
        "quote_currency": instrument.quote_currency,
    }


def _bar_to_record(bar: Bar) -> dict[str, Any]:
    return {
        **_base_record(bar.timestamp, bar.instrument),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "metadata_json": _metadata_to_json(bar.metadata),
    }


def _trade_to_record(trade: Trade) -> dict[str, Any]:
    return {
        **_base_record(trade.timestamp, trade.instrument),
        "trade_id": trade.trade_id,
        "price": trade.price,
        "quantity": trade.quantity,
        "side": trade.side.value if trade.side else "",
        "notional": trade.notional,
        "metadata_json": _metadata_to_json(trade.metadata),
    }


def _quote_to_record(quote: Quote) -> dict[str, Any]:
    return {
        **_base_record(quote.timestamp, quote.instrument),
        "bid_price": quote.bid_price,
        "bid_size": quote.bid_size,
        "ask_price": quote.ask_price,
        "ask_size": quote.ask_size,
        "mid_price": quote.mid_price,
        "spread": quote.spread,
        "metadata_json": _metadata_to_json(quote.metadata),
    }


def _order_book_snapshot_to_records(snapshot: OrderBookSnapshot) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for levels in (snapshot.bids, snapshot.asks):
        for level in levels:
            rows.append(
                {
                    **_base_record(snapshot.timestamp, snapshot.instrument),
                    "sequence_id": snapshot.sequence_id,
                    "side": level.side.value,
                    "level": level.level,
                    "price": level.price,
                    "quantity": level.quantity,
                    "metadata_json": _metadata_to_json(snapshot.metadata),
                }
            )
    return rows


def _record_to_bar(record: dict[str, Any]) -> Bar:
    return Bar(
        timestamp=record["timestamp"],
        instrument=_instrument_from_record(record),
        open=float(record["open"]),
        high=float(record["high"]),
        low=float(record["low"]),
        close=float(record["close"]),
        volume=float(record["volume"]),
        metadata=_metadata_from_record(record),
    )


def _record_to_trade(record: dict[str, Any]) -> Trade:
    side_value = str(record.get("side", "")).strip()
    side = Side(side_value) if side_value else None
    return Trade(
        timestamp=record["timestamp"],
        instrument=_instrument_from_record(record),
        trade_id=str(record["trade_id"]),
        price=float(record["price"]),
        quantity=float(record["quantity"]),
        side=side,
        metadata=_metadata_from_record(record),
    )


def _record_to_quote(record: dict[str, Any]) -> Quote:
    return Quote(
        timestamp=record["timestamp"],
        instrument=_instrument_from_record(record),
        bid_price=float(record["bid_price"]),
        bid_size=float(record["bid_size"]),
        ask_price=float(record["ask_price"]),
        ask_size=float(record["ask_size"]),
        metadata=_metadata_from_record(record),
    )


def _records_to_order_book_snapshots(rows: Sequence[dict[str, Any]]) -> tuple[OrderBookSnapshot, ...]:
    grouped: dict[tuple[Any, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["timestamp"], str(row["symbol"]), str(row["sequence_id"]))].append(row)

    snapshots: list[OrderBookSnapshot] = []
    for grouped_rows in grouped.values():
        sample = grouped_rows[0]
        bids: list[OrderBookLevel] = []
        asks: list[OrderBookLevel] = []
        for row in grouped_rows:
            level = OrderBookLevel(
                side=Side(str(row["side"])),
                level=int(row["level"]),
                price=float(row["price"]),
                quantity=float(row["quantity"]),
            )
            if level.side is Side.BUY:
                bids.append(level)
            else:
                asks.append(level)
        snapshots.append(
            OrderBookSnapshot(
                timestamp=sample["timestamp"],
                instrument=_instrument_from_record(sample),
                sequence_id=str(sample["sequence_id"]),
                bids=tuple(sorted(bids, key=lambda level: level.level)),
                asks=tuple(sorted(asks, key=lambda level: level.level)),
                metadata=_metadata_from_record(sample),
            )
        )
    return tuple(sorted(snapshots, key=lambda snapshot: (snapshot.timestamp, snapshot.sequence_id)))


def _instrument_from_record(record: dict[str, Any]) -> Instrument:
    return Instrument(
        symbol=str(record["symbol"]),
        venue=str(record["venue"]),
        asset_class=AssetClass(str(record["asset_class"])),
        quote_currency=str(record.get("quote_currency", "USD")),
    )


def _require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.dataset as ds
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyarrow is required for ParquetMarketDataStore. Install project dependencies first.") from exc
    return pa, ds


def _metadata_to_json(metadata: Any) -> str:
    return json.dumps(dict(metadata or {}), sort_keys=True)


def _metadata_from_record(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("metadata_json", "")
    if value in {"", None}:
        return {}
    if isinstance(value, dict):
        return dict(value)
    return dict(json.loads(str(value)))


def _merge_by_key(
    existing: Sequence[Any],
    incoming: Sequence[Any],
    *,
    key,
    sort_key,
) -> tuple[Any, ...]:
    merged: dict[Any, Any] = {}
    for item in existing:
        merged[key(item)] = item
    for item in incoming:
        merged[key(item)] = item
    return tuple(sorted(merged.values(), key=sort_key))
