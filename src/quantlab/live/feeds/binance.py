from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from quantlab.core.enums import AssetClass, Side
from quantlab.core.models import Bar, Instrument, OrderBookLevel, OrderBookSnapshot, Quote, Trade
from quantlab.data.adapters import BinanceMarketDataAdapter, TradeFieldMap


MarketEvent = Bar | Trade | Quote | OrderBookSnapshot


class BinanceRealtimeChannel(StrEnum):
    AGG_TRADE = "aggTrade"
    TRADE = "trade"
    BOOK_TICKER = "bookTicker"
    DEPTH = "depth"
    PARTIAL_DEPTH = "partialDepth"
    KLINE = "kline"


@dataclass(frozen=True, slots=True)
class BinanceStreamSubscription:
    symbol: str
    channel: BinanceRealtimeChannel
    depth: int | None = None
    interval: str | None = None
    update_speed_ms: int | None = None

    @property
    def stream_name(self) -> str:
        normalized_symbol = self.symbol.lower()
        stream: str
        if self.channel is BinanceRealtimeChannel.KLINE:
            if not self.interval:
                raise ValueError("interval is required for kline subscriptions")
            stream = f"{normalized_symbol}@kline_{self.interval}"
        elif self.channel is BinanceRealtimeChannel.PARTIAL_DEPTH:
            if self.depth is None:
                raise ValueError("depth is required for partial depth subscriptions")
            stream = f"{normalized_symbol}@depth{self.depth}"
        elif self.channel is BinanceRealtimeChannel.DEPTH:
            stream = f"{normalized_symbol}@depth"
        else:
            stream = f"{normalized_symbol}@{self.channel.value}"
        if self.update_speed_ms == 100:
            return f"{stream}@100ms"
        return stream


class BinanceRealtimeNormalizationError(ValueError):
    def __init__(self, message: str, payload: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.payload = dict(payload)


def _infer_quote_currency(symbol: str) -> str:
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USD", "BTC", "ETH", "BNB", "EUR"):
        if symbol.endswith(suffix):
            return suffix
    return "USD"


def default_binance_instrument_factory(symbol: str) -> Instrument:
    normalized = symbol.upper()
    return Instrument(
        symbol=normalized,
        venue="BINANCE",
        asset_class=AssetClass.CRYPTO,
        quote_currency=_infer_quote_currency(normalized),
    )


class BinanceRealtimeNormalizer:
    """Normalize Binance websocket payloads into platform-standard market events."""

    def __init__(
        self,
        instrument_factory: Callable[[str], Instrument] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._adapter = BinanceMarketDataAdapter()
        self._instrument_factory = instrument_factory or default_binance_instrument_factory
        self._clock = clock or (lambda: datetime.now(UTC))

    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        instrument: Instrument | None = None,
    ) -> MarketEvent:
        stream, data = self._unwrap_payload(payload)
        event_type = self._event_type_for(stream, data)
        resolved_instrument = instrument or self._resolve_instrument(data)

        if event_type == BinanceRealtimeChannel.AGG_TRADE.value:
            return self._normalize_agg_trade(data, resolved_instrument, stream)
        if event_type == BinanceRealtimeChannel.TRADE.value:
            return self._normalize_trade(data, resolved_instrument, stream)
        if event_type == BinanceRealtimeChannel.BOOK_TICKER.value:
            return self._normalize_book_ticker(data, resolved_instrument, stream)
        if event_type == BinanceRealtimeChannel.KLINE.value:
            return self._normalize_kline(data, resolved_instrument, stream)
        if event_type == BinanceRealtimeChannel.PARTIAL_DEPTH.value:
            return self._normalize_partial_depth(data, resolved_instrument, stream)
        if event_type == "depthUpdate":
            return self._normalize_depth_update(data, resolved_instrument, stream)
        raise BinanceRealtimeNormalizationError(f"unsupported websocket event type: {event_type}", data)

    def _unwrap_payload(self, payload: Mapping[str, Any]) -> tuple[str | None, Mapping[str, Any]]:
        stream = payload.get("stream")
        data = payload.get("data", payload)
        if not isinstance(data, Mapping):
            raise BinanceRealtimeNormalizationError("websocket payload must contain a mapping", payload)
        if data.get("e") == "error":
            raise BinanceRealtimeNormalizationError(data.get("m", "binance websocket error"), data)
        return str(stream) if stream is not None else None, data

    def _event_type_for(self, stream: str | None, data: Mapping[str, Any]) -> str:
        explicit_type = data.get("e")
        if explicit_type:
            return str(explicit_type)
        if stream:
            stream_type = stream.split("@", 1)[-1]
            if stream_type.startswith("kline_"):
                return BinanceRealtimeChannel.KLINE.value
            if stream_type.startswith("depth"):
                if any(character.isdigit() for character in stream_type):
                    return BinanceRealtimeChannel.PARTIAL_DEPTH.value
                return "depthUpdate"
            return stream_type
        bids = data.get("b")
        if "k" in data:
            return BinanceRealtimeChannel.KLINE.value
        if "lastUpdateId" in data and all(field in data for field in ("bids", "asks")):
            return BinanceRealtimeChannel.PARTIAL_DEPTH.value
        if (
            isinstance(bids, Sequence)
            and not isinstance(bids, (str, bytes))
            and bids
            and isinstance(bids[0], Sequence)
            and not isinstance(bids[0], (str, bytes))
        ):
            return "depthUpdate"
        if all(field in data for field in ("b", "B", "a", "A")):
            return BinanceRealtimeChannel.BOOK_TICKER.value
        raise BinanceRealtimeNormalizationError("unable to infer websocket event type", data)

    def _resolve_instrument(self, data: Mapping[str, Any]) -> Instrument:
        symbol = data.get("s")
        if not symbol:
            raise BinanceRealtimeNormalizationError("websocket payload missing symbol", data)
        return self._instrument_factory(str(symbol))

    def _normalize_agg_trade(
        self,
        data: Mapping[str, Any],
        instrument: Instrument,
        stream: str | None,
    ) -> Trade:
        timestamp_value, timestamp_source = self._timestamp_value(data, "T", "E")
        trade = self._adapter.normalize_agg_trades(
            [
                {
                    "a": data["a"],
                    "p": data["p"],
                    "q": data["q"],
                    "T": timestamp_value,
                    "m": data.get("m"),
                }
            ],
            instrument,
        )[0]
        return replace(
            trade,
            metadata={
                **trade.metadata,
                "stream": stream,
                "event_type": BinanceRealtimeChannel.AGG_TRADE.value,
                "timestamp_source": timestamp_source,
            },
        )

    def _normalize_trade(
        self,
        data: Mapping[str, Any],
        instrument: Instrument,
        stream: str | None,
    ) -> Trade:
        timestamp_value, timestamp_source = self._timestamp_value(data, "T", "E")
        trade = self._adapter.normalize_trades(
            [
                {
                    "t": data["t"],
                    "p": data["p"],
                    "q": data["q"],
                    "T": timestamp_value,
                    "m": data.get("m"),
                }
            ],
            instrument,
            TradeFieldMap(timestamp="T", price="p", quantity="q", trade_id="t", side="m"),
        )[0]
        return replace(
            trade,
            metadata={
                **trade.metadata,
                "stream": stream,
                "event_type": BinanceRealtimeChannel.TRADE.value,
                "timestamp_source": timestamp_source,
            },
        )

    def _normalize_book_ticker(
        self,
        data: Mapping[str, Any],
        instrument: Instrument,
        stream: str | None,
    ) -> Quote:
        timestamp_value, timestamp_source = self._timestamp_value(data, "E", "T")
        quote = self._adapter.normalize_book_ticker(
            [
                {
                    "T": timestamp_value,
                    "b": data["b"],
                    "B": data["B"],
                    "a": data["a"],
                    "A": data["A"],
                }
            ],
            instrument,
        )[0]
        return replace(
            quote,
            metadata={
                **quote.metadata,
                "stream": stream,
                "event_type": BinanceRealtimeChannel.BOOK_TICKER.value,
                "timestamp_source": timestamp_source,
                "sequence_id": str(data.get("u", "")),
            },
        )

    def _normalize_kline(
        self,
        data: Mapping[str, Any],
        instrument: Instrument,
        stream: str | None,
    ) -> Bar:
        kline = data["k"]
        timestamp = self._adapter._coerce_timestamp(kline["t"])  # noqa: SLF001
        return Bar(
            timestamp=timestamp,
            instrument=instrument,
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            metadata={
                "vendor": "binance",
                "stream": stream,
                "event_type": BinanceRealtimeChannel.KLINE.value,
                "interval": str(kline["i"]),
                "event_time": str(data.get("E", "")),
                "close_time": str(kline.get("T", "")),
                "quote_asset_volume": str(kline.get("q", "")),
                "number_of_trades": str(kline.get("n", "")),
                "taker_buy_base_asset_volume": str(kline.get("V", "")),
                "taker_buy_quote_asset_volume": str(kline.get("Q", "")),
                "is_closed": str(bool(kline.get("x", False))).lower(),
                "timestamp_source": "exchange" if data.get("E") is not None else "gateway_receive",
            },
        )

    def _normalize_depth_update(
        self,
        data: Mapping[str, Any],
        instrument: Instrument,
        stream: str | None,
    ) -> OrderBookSnapshot:
        timestamp_value, timestamp_source = self._timestamp_value(data, "E")
        snapshot = self._adapter.normalize_depth_updates(
            [
                {
                    "E": timestamp_value,
                    "u": data["u"],
                    "b": data.get("b", ()),
                    "a": data.get("a", ()),
                }
            ],
            instrument,
        )[0]
        return replace(
            snapshot,
            metadata={
                **snapshot.metadata,
                "stream": stream,
                "event_type": "depthUpdate",
                "timestamp_source": timestamp_source,
                "first_update_id": str(data.get("U", "")),
            },
        )

    def _normalize_partial_depth(
        self,
        data: Mapping[str, Any],
        instrument: Instrument,
        stream: str | None,
    ) -> OrderBookSnapshot:
        timestamp_value, timestamp_source = self._timestamp_value(data, "E")
        timestamp = self._adapter._coerce_timestamp(timestamp_value)  # noqa: SLF001
        bids = tuple(
            OrderBookLevel(side=Side.BUY, level=index + 1, price=float(price), quantity=float(quantity))
            for index, (price, quantity) in enumerate(data.get("bids", ()))
        )
        asks = tuple(
            OrderBookLevel(side=Side.SELL, level=index + 1, price=float(price), quantity=float(quantity))
            for index, (price, quantity) in enumerate(data.get("asks", ()))
        )
        return OrderBookSnapshot(
            timestamp=timestamp,
            instrument=instrument,
            sequence_id=str(data["lastUpdateId"]),
            bids=bids,
            asks=asks,
            metadata={
                "vendor": "binance",
                "stream": stream,
                "event_type": BinanceRealtimeChannel.PARTIAL_DEPTH.value,
                "timestamp_source": timestamp_source,
            },
        )

    def _timestamp_value(self, data: Mapping[str, Any], *keys: str) -> tuple[int | float | datetime, str]:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key], "exchange"
        return int(self._clock().timestamp() * 1000), "gateway_receive"


class PythonBinanceMultiplexFeed:
    """Thin wrapper around python-binance ThreadedWebsocketManager for public market-data streams."""

    def __init__(
        self,
        subscriptions: Sequence[BinanceStreamSubscription],
        on_event: Callable[[MarketEvent], None],
        on_error: Callable[[Mapping[str, Any]], None] | None = None,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        tld: str = "com",
        normalizer: BinanceRealtimeNormalizer | None = None,
        manager_factory: Callable[[], Any] | None = None,
    ) -> None:
        if not subscriptions:
            raise ValueError("at least one websocket subscription is required")
        self._subscriptions = tuple(subscriptions)
        self._on_event = on_event
        self._on_error = on_error
        self._api_key = api_key
        self._api_secret = api_secret
        self._tld = tld
        self._normalizer = normalizer or BinanceRealtimeNormalizer()
        self._manager_factory = manager_factory
        self._manager: Any | None = None
        self._socket_name: str | None = None

    @property
    def stream_names(self) -> tuple[str, ...]:
        return tuple(subscription.stream_name for subscription in self._subscriptions)

    def start(self) -> str:
        if self._manager is not None and self._socket_name is not None:
            return self._socket_name
        self._manager = self._manager_factory() if self._manager_factory else self._build_manager()
        self._manager.start()
        self._socket_name = self._manager.start_multiplex_socket(
            callback=self.dispatch_message,
            streams=list(self.stream_names),
        )
        return self._socket_name

    def stop(self) -> None:
        if self._manager is None:
            return
        if self._socket_name is not None and hasattr(self._manager, "stop_socket"):
            self._manager.stop_socket(self._socket_name)
        if hasattr(self._manager, "stop"):
            self._manager.stop()
        self._manager = None
        self._socket_name = None

    def join(self) -> None:
        if self._manager is not None and hasattr(self._manager, "join"):
            self._manager.join()

    def dispatch_message(self, payload: Mapping[str, Any]) -> None:
        try:
            event = self._normalizer.normalize_payload(payload)
        except BinanceRealtimeNormalizationError as exc:
            if self._on_error is not None:
                self._on_error(exc.payload)
                return
            raise
        self._on_event(event)

    def _build_manager(self) -> Any:
        try:
            from binance import ThreadedWebsocketManager
        except ImportError as exc:
            raise RuntimeError(
                "python-binance is not installed. Install it with `pip install -e .[live]` or `pip install python-binance`."
            ) from exc
        manager_kwargs: dict[str, Any] = {"tld": self._tld}
        if self._api_key is not None:
            manager_kwargs["api_key"] = self._api_key
        if self._api_secret is not None:
            manager_kwargs["api_secret"] = self._api_secret
        return ThreadedWebsocketManager(**manager_kwargs)
