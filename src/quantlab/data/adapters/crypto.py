from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from quantlab.core.enums import Side
from quantlab.core.models import Instrument, OrderBookLevel, OrderBookSnapshot, Quote, Trade
from quantlab.data.adapters.base import MappedVendorMarketDataAdapter, QuoteFieldMap, TradeFieldMap


class BinanceMarketDataAdapter(MappedVendorMarketDataAdapter):
    """Normalization skeleton for Binance-style historical exports and websocket payloads."""

    def __init__(self) -> None:
        super().__init__(
            vendor_name="binance",
            timestamp_unit="ms",
            side_map={False: Side.BUY, True: Side.SELL},
        )

    def normalize_agg_trades(
        self,
        rows: Sequence[Mapping[str, Any]],
        instrument: Instrument,
    ) -> tuple[Trade, ...]:
        return self.normalize_trades(
            rows,
            instrument,
            TradeFieldMap(timestamp="T", price="p", quantity="q", trade_id="a", side="m"),
        )

    def normalize_book_ticker(
        self,
        rows: Sequence[Mapping[str, Any]],
        instrument: Instrument,
    ) -> tuple[Quote, ...]:
        return self.normalize_quotes(
            rows,
            instrument,
            QuoteFieldMap(timestamp="T", bid_price="b", bid_size="B", ask_price="a", ask_size="A"),
        )

    def normalize_depth_updates(
        self,
        rows: Sequence[Mapping[str, Any]],
        instrument: Instrument,
    ) -> tuple[OrderBookSnapshot, ...]:
        snapshots: list[OrderBookSnapshot] = []
        for row in rows:
            timestamp = self._coerce_timestamp(row["E"])
            sequence_id = str(row["u"])
            bids = tuple(
                OrderBookLevel(side=Side.BUY, level=index + 1, price=float(price), quantity=float(quantity))
                for index, (price, quantity) in enumerate(row.get("b", ()))
            )
            asks = tuple(
                OrderBookLevel(side=Side.SELL, level=index + 1, price=float(price), quantity=float(quantity))
                for index, (price, quantity) in enumerate(row.get("a", ()))
            )
            snapshots.append(
                OrderBookSnapshot(
                    timestamp=timestamp,
                    instrument=instrument,
                    sequence_id=sequence_id,
                    bids=bids,
                    asks=asks,
                    metadata={"vendor": self.vendor_name},
                )
            )
        return tuple(sorted(snapshots, key=lambda snapshot: (snapshot.timestamp, snapshot.sequence_id)))
