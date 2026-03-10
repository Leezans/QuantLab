from __future__ import annotations

from enum import StrEnum


class AssetClass(StrEnum):
    CRYPTO = "crypto"
    EQUITY = "equity"
    FUTURE = "future"
    OPTION = "option"


class DataFrequency(StrEnum):
    TICK = "tick"
    ONE_MINUTE = "1m"
    FIVE_MINUTE = "5m"
    HOURLY = "1h"
    DAILY = "1d"


class DatasetKind(StrEnum):
    BAR = "bar"
    TRADE = "trade"
    QUOTE = "quote"
    ORDER_BOOK_SNAPSHOT = "order_book_snapshot"
    FEATURE_FRAME = "feature_frame"


class StorageTier(StrEnum):
    RAW = "raw"
    CACHE = "cache"
    NORMALIZED = "normalized"
    CURATED = "curated"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class SignalDirection(StrEnum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"
