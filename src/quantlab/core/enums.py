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
    BAR = "bar"                                     # K线
    TRADE = "trade"                                 # 成交
    QUOTE = "quote"                                 # best bid / ask 报价
    ORDER_BOOK_SNAPSHOT = "order_book_snapshot"     # 全订单簿
    FEATURE_FRAME = "feature_frame"                 # 因子矩阵


class StorageTier(StrEnum):
    RAW = "raw"                 # 原始数据，未经过任何处理
    CACHE = "cache"             # 临时数据，通常是从原始数据中提取的子集，用于加速特定查询或计算
    NORMALIZED = "normalized"   # 经过清洗和标准化处理的数据，适合分析和建模
    CURATED = "curated"         # 精心挑选和处理的数据，通常是针对特定研究问题或策略开发的子集


class Side(StrEnum):
    BUY = "buy"                                     # 买入
    SELL = "sell"                                   # 卖出


class OrderType(StrEnum):
    MARKET = "market"                               # 市价单
    LIMIT = "limit"                                 # 限价单


class SignalDirection(StrEnum):
    LONG = "long"       # 做多
    SHORT = "short"     # 做空
    FLAT = "flat"       # 空仓
