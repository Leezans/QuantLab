from __future__ import annotations

import unittest
from datetime import UTC, datetime
from typing import Any

from quantlab.core.models import Bar, OrderBookSnapshot, Quote, Trade
from quantlab.live import (
    BinanceRealtimeChannel,
    BinanceRealtimeNormalizer,
    BinanceStreamSubscription,
    PythonBinanceMultiplexFeed,
)


class BinanceStreamSubscriptionTestCase(unittest.TestCase):
    def test_builds_expected_stream_names(self) -> None:
        self.assertEqual(
            BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.AGG_TRADE).stream_name,
            "btcusdt@aggTrade",
        )
        self.assertEqual(
            BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.BOOK_TICKER).stream_name,
            "btcusdt@bookTicker",
        )
        self.assertEqual(
            BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.DEPTH, depth=5).stream_name,
            "btcusdt@depth",
        )
        self.assertEqual(
            BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.PARTIAL_DEPTH, depth=5).stream_name,
            "btcusdt@depth5",
        )
        self.assertEqual(
            BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.KLINE, interval="1m").stream_name,
            "btcusdt@kline_1m",
        )


class BinanceRealtimeNormalizerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.fixed_now = datetime(2024, 1, 1, 0, 0, 5, tzinfo=UTC)
        self.normalizer = BinanceRealtimeNormalizer(clock=lambda: self.fixed_now)

    def test_normalizes_multiplex_agg_trade_payload(self) -> None:
        event = self.normalizer.normalize_payload(
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1704067200000,
                    "s": "BTCUSDT",
                    "a": 1001,
                    "p": "42000.10",
                    "q": "0.005",
                    "m": False,
                },
            }
        )
        self.assertIsInstance(event, Trade)
        self.assertEqual(event.instrument.symbol, "BTCUSDT")
        self.assertEqual(event.trade_id, "1001")
        self.assertEqual(event.metadata["stream"], "btcusdt@aggTrade")
        self.assertEqual(event.metadata["timestamp_source"], "exchange")

    def test_normalizes_book_ticker_without_exchange_timestamp(self) -> None:
        event = self.normalizer.normalize_payload(
            {
                "stream": "btcusdt@bookTicker",
                "data": {
                    "u": 4001,
                    "s": "BTCUSDT",
                    "b": "42000.00",
                    "B": "1.20",
                    "a": "42000.25",
                    "A": "0.80",
                },
            }
        )
        self.assertIsInstance(event, Quote)
        self.assertEqual(event.instrument.quote_currency, "USDT")
        self.assertEqual(event.timestamp, self.fixed_now)
        self.assertEqual(event.metadata["timestamp_source"], "gateway_receive")
        self.assertEqual(event.metadata["sequence_id"], "4001")

    def test_normalizes_depth_update_payload(self) -> None:
        event = self.normalizer.normalize_payload(
            {
                "stream": "btcusdt@depth",
                "data": {
                    "e": "depthUpdate",
                    "E": 1704067201000,
                    "U": 501,
                    "u": 505,
                    "s": "BTCUSDT",
                    "b": [["42000.00", "1.50"], ["41999.90", "2.00"]],
                    "a": [["42000.25", "0.80"], ["42000.40", "1.20"]],
                },
            }
        )
        self.assertIsInstance(event, OrderBookSnapshot)
        self.assertEqual(event.sequence_id, "505")
        self.assertEqual(event.metadata["first_update_id"], "501")
        self.assertEqual(event.bids[0].price, 42000.0)
        self.assertEqual(event.asks[0].price, 42000.25)

    def test_normalizes_partial_depth_payload(self) -> None:
        event = self.normalizer.normalize_payload(
            {
                "stream": "btcusdt@depth5",
                "data": {
                    "lastUpdateId": 7001,
                    "s": "BTCUSDT",
                    "bids": [["42000.00", "1.50"], ["41999.90", "2.00"]],
                    "asks": [["42000.25", "0.80"], ["42000.40", "1.20"]],
                },
            }
        )
        self.assertIsInstance(event, OrderBookSnapshot)
        self.assertEqual(event.sequence_id, "7001")
        self.assertEqual(event.metadata["event_type"], "partialDepth")
        self.assertEqual(event.bids[0].quantity, 1.5)

    def test_normalizes_kline_payload(self) -> None:
        event = self.normalizer.normalize_payload(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "E": 1704067201000,
                    "s": "BTCUSDT",
                    "k": {
                        "t": 1704067200000,
                        "T": 1704067259999,
                        "i": "1m",
                        "o": "42000.00",
                        "c": "42010.00",
                        "h": "42020.00",
                        "l": "41995.00",
                        "v": "12.34",
                        "q": "518000.12",
                        "n": 120,
                        "V": "6.10",
                        "Q": "256000.11",
                        "x": False,
                    },
                },
            }
        )
        self.assertIsInstance(event, Bar)
        self.assertEqual(event.metadata["interval"], "1m")
        self.assertEqual(event.close, 42010.0)
        self.assertEqual(event.volume, 12.34)


class FakeThreadedWebsocketManager:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.streams: list[str] = []
        self.socket_name = "socket-1"
        self.callback = None
        self.stopped_socket_name: str | None = None

    def start(self) -> None:
        self.started = True

    def start_multiplex_socket(self, callback, streams):  # type: ignore[no-untyped-def]
        self.callback = callback
        self.streams = list(streams)
        return self.socket_name

    def stop_socket(self, socket_name: str) -> None:
        self.stopped_socket_name = socket_name

    def stop(self) -> None:
        self.stopped = True


class PythonBinanceMultiplexFeedTestCase(unittest.TestCase):
    def test_dispatches_events_and_errors_through_callbacks(self) -> None:
        manager = FakeThreadedWebsocketManager()
        events: list[Any] = []
        errors: list[Any] = []
        feed = PythonBinanceMultiplexFeed(
            subscriptions=[
                BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.AGG_TRADE),
                BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.BOOK_TICKER),
            ],
            on_event=events.append,
            on_error=errors.append,
            manager_factory=lambda: manager,
            normalizer=BinanceRealtimeNormalizer(clock=lambda: datetime(2024, 1, 1, tzinfo=UTC)),
        )

        socket_name = feed.start()
        self.assertEqual(socket_name, "socket-1")
        self.assertTrue(manager.started)
        self.assertEqual(manager.streams, ["btcusdt@aggTrade", "btcusdt@bookTicker"])

        manager.callback(
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1704067200000,
                    "s": "BTCUSDT",
                    "a": 1001,
                    "p": "42000.10",
                    "q": "0.005",
                    "m": False,
                },
            }
        )
        manager.callback({"stream": "btcusdt@aggTrade", "data": {"e": "error", "type": "RuntimeError", "m": "boom"}})

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], Trade)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["m"], "boom")

        feed.stop()
        self.assertEqual(manager.stopped_socket_name, "socket-1")
        self.assertTrue(manager.stopped)


if __name__ == "__main__":
    unittest.main()
