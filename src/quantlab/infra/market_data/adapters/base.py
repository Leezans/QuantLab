from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from quantlab.domain.data.enums import Side
from quantlab.domain.data.models import Instrument, OrderBookLevel, OrderBookSnapshot, Quote, Trade


@dataclass(frozen=True, slots=True)
class TradeFieldMap:
    timestamp: str
    price: str
    quantity: str
    trade_id: str | None = None
    side: str | None = None


@dataclass(frozen=True, slots=True)
class QuoteFieldMap:
    timestamp: str
    bid_price: str
    bid_size: str
    ask_price: str
    ask_size: str


@dataclass(frozen=True, slots=True)
class OrderBookFieldMap:
    timestamp: str
    sequence_id: str
    side: str
    level: str
    price: str
    quantity: str


class MappedVendorMarketDataAdapter:
    """Reusable field-mapping normalizer for vendor-specific historical exports."""

    def __init__(
        self,
        vendor_name: str,
        timestamp_unit: str = "ms",
        side_map: Mapping[Any, Side] | None = None,
    ) -> None:
        self.vendor_name = vendor_name
        self.timestamp_unit = timestamp_unit
        self.side_map = dict(side_map or {})

    def normalize_trades(
        self,
        rows: Sequence[Mapping[str, Any]],
        instrument: Instrument,
        field_map: TradeFieldMap,
    ) -> tuple[Trade, ...]:
        trades: list[Trade] = []
        for index, row in enumerate(rows):
            side_value = row.get(field_map.side) if field_map.side else None
            side = self._coerce_side(side_value) if side_value is not None else None
            trade_id_value = row.get(field_map.trade_id) if field_map.trade_id else index
            trades.append(
                Trade(
                    timestamp=self._coerce_timestamp(row[field_map.timestamp]),
                    instrument=instrument,
                    trade_id=str(trade_id_value),
                    price=float(row[field_map.price]),
                    quantity=float(row[field_map.quantity]),
                    side=side,
                    metadata={"vendor": self.vendor_name},
                )
            )
        return tuple(sorted(trades, key=lambda trade: (trade.timestamp, trade.trade_id)))

    def normalize_quotes(
        self,
        rows: Sequence[Mapping[str, Any]],
        instrument: Instrument,
        field_map: QuoteFieldMap,
    ) -> tuple[Quote, ...]:
        quotes = [
            Quote(
                timestamp=self._coerce_timestamp(row[field_map.timestamp]),
                instrument=instrument,
                bid_price=float(row[field_map.bid_price]),
                bid_size=float(row[field_map.bid_size]),
                ask_price=float(row[field_map.ask_price]),
                ask_size=float(row[field_map.ask_size]),
                metadata={"vendor": self.vendor_name},
            )
            for row in rows
        ]
        return tuple(sorted(quotes, key=lambda quote: quote.timestamp))

    def normalize_order_book_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        instrument: Instrument,
        field_map: OrderBookFieldMap,
    ) -> tuple[OrderBookSnapshot, ...]:
        grouped: dict[tuple[datetime, str], list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            timestamp = self._coerce_timestamp(row[field_map.timestamp])
            sequence_id = str(row[field_map.sequence_id])
            grouped[(timestamp, sequence_id)].append(row)

        snapshots: list[OrderBookSnapshot] = []
        for (timestamp, sequence_id), grouped_rows in sorted(grouped.items(), key=lambda item: item[0]):
            bids: list[OrderBookLevel] = []
            asks: list[OrderBookLevel] = []
            for row in grouped_rows:
                level = OrderBookLevel(
                    side=self._coerce_side(row[field_map.side]),
                    level=int(row[field_map.level]),
                    price=float(row[field_map.price]),
                    quantity=float(row[field_map.quantity]),
                )
                if level.side is Side.BUY:
                    bids.append(level)
                else:
                    asks.append(level)
            snapshots.append(
                OrderBookSnapshot(
                    timestamp=timestamp,
                    instrument=instrument,
                    sequence_id=sequence_id,
                    bids=tuple(sorted(bids, key=lambda level: level.level)),
                    asks=tuple(sorted(asks, key=lambda level: level.level)),
                    metadata={"vendor": self.vendor_name},
                )
            )
        return tuple(snapshots)

    def _coerce_timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone(UTC)
        if isinstance(value, (int, float)):
            if self.timestamp_unit == "ms":
                return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)
            if self.timestamp_unit == "us":
                return datetime.fromtimestamp(float(value) / 1_000_000.0, tz=UTC)
            return datetime.fromtimestamp(float(value), tz=UTC)
        if isinstance(value, str):
            if value.isdigit():
                return self._coerce_timestamp(int(value))
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        raise TypeError(f"unsupported timestamp value: {value!r}")

    def _coerce_side(self, value: Any) -> Side:
        if isinstance(value, Side):
            return value
        if value in self.side_map:
            return self.side_map[value]
        normalized = str(value).strip().lower()
        if normalized in {"buy", "bid", "b", "long"}:
            return Side.BUY
        if normalized in {"sell", "ask", "a", "short"}:
            return Side.SELL
        raise ValueError(f"unsupported side value for {self.vendor_name}: {value!r}")
