from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import exp
from pathlib import Path
from statistics import pstdev

from quantlab.core.enums import DatasetKind, Side, StorageTier
from quantlab.core.models import Bar, OrderBookLevel, OrderBookSnapshot, Trade
from quantlab.data.catalog import DataCatalog, DatasetRef
from quantlab.data.schemas import schema_for_kind
from quantlab.data.stores import ParquetMarketDataStore


@dataclass(frozen=True, slots=True)
class SyntheticDepthConfig:
    levels: int = 5
    base_spread_bps: float = 2.0
    volatility_spread_multiplier: float = 2_400.0
    imbalance_skew: float = 0.35
    level_decay: float = 0.75
    depth_notional_fraction: float = 0.04
    min_level_notional: float = 2_500.0


@dataclass(frozen=True, slots=True)
class SyntheticDepthMaterializationResult:
    snapshots: tuple[OrderBookSnapshot, ...]
    datasets: tuple[DatasetRef, ...]


class SyntheticDepthSnapshotBuilder:
    """Derive research-grade depth proxies from bars and aggressive trade flow."""

    def __init__(self, config: SyntheticDepthConfig | None = None) -> None:
        self._config = config or SyntheticDepthConfig()

    @property
    def config(self) -> SyntheticDepthConfig:
        return self._config

    def build(
        self,
        bars: Sequence[Bar],
        trades: Sequence[Trade],
    ) -> tuple[OrderBookSnapshot, ...]:
        by_symbol_bars: dict[str, list[Bar]] = defaultdict(list)
        by_symbol_trades: dict[str, list[Trade]] = defaultdict(list)
        for bar in sorted(bars, key=lambda value: (value.instrument.symbol, value.timestamp)):
            by_symbol_bars[bar.instrument.symbol].append(bar)
        for trade in sorted(trades, key=lambda value: (value.instrument.symbol, value.timestamp, value.trade_id)):
            by_symbol_trades[trade.instrument.symbol].append(trade)

        snapshots: list[OrderBookSnapshot] = []
        for symbol, symbol_bars in sorted(by_symbol_bars.items()):
            trade_buckets = _bucket_trades_by_bar(symbol_bars, by_symbol_trades.get(symbol, ()))
            for bar in symbol_bars:
                summary = trade_buckets.get(bar.timestamp, _empty_trade_summary())
                snapshots.append(self._build_snapshot(bar, summary))
        return tuple(sorted(snapshots, key=lambda value: (value.timestamp, value.instrument.symbol, value.sequence_id)))

    def _build_snapshot(self, bar: Bar, trade_summary: Mapping[str, float]) -> OrderBookSnapshot:
        config = self._config
        mid_price = max(bar.close, 1e-9)
        imbalance = float(trade_summary["signed_quantity_imbalance"])
        dispersion = float(trade_summary["trade_price_dispersion"])
        bar_volatility = max(0.0, (bar.high - bar.low) / mid_price)
        spread_bps = max(
            config.base_spread_bps,
            config.base_spread_bps
            + (bar_volatility * config.volatility_spread_multiplier)
            + (dispersion * 10_000.0 * 0.5)
            + (abs(imbalance) * 1.5),
        )

        total_notional = max(
            float(trade_summary["total_notional"]) * config.depth_notional_fraction,
            bar.volume * mid_price * 0.015,
            config.min_level_notional * config.levels,
        )
        depth_weights = _level_weights(config.levels, config.level_decay)
        bid_share = _clamp(0.5 + (0.5 * imbalance), 0.15, 0.85)
        ask_share = 1.0 - bid_share

        bids: list[OrderBookLevel] = []
        asks: list[OrderBookLevel] = []
        for level in range(1, config.levels + 1):
            distance_multiplier = 0.5 + (0.8 * (level - 1))
            level_spread_bps = spread_bps * distance_multiplier
            bid_offset_bps = level_spread_bps * (1.0 - max(imbalance, 0.0) * config.imbalance_skew)
            ask_offset_bps = level_spread_bps * (1.0 + min(imbalance, 0.0) * config.imbalance_skew)
            bid_price = mid_price * (1.0 - (bid_offset_bps / 10_000.0))
            ask_price = mid_price * (1.0 + (ask_offset_bps / 10_000.0))
            level_notional = total_notional * depth_weights[level - 1]
            bid_notional = level_notional * bid_share
            ask_notional = level_notional * ask_share
            bids.append(
                OrderBookLevel(
                    side=Side.BUY,
                    level=level,
                    price=max(bid_price, 1e-9),
                    quantity=max(bid_notional / max(bid_price, 1e-9), 1e-9),
                )
            )
            asks.append(
                OrderBookLevel(
                    side=Side.SELL,
                    level=level,
                    price=max(ask_price, 1e-9),
                    quantity=max(ask_notional / max(ask_price, 1e-9), 1e-9),
                )
            )

        top_depth_notional = (
            (bids[0].price * bids[0].quantity)
            + (asks[0].price * asks[0].quantity)
            if bids and asks
            else 0.0
        )
        liquidity_score = top_depth_notional / max(spread_bps, 1e-9)
        return OrderBookSnapshot(
            timestamp=bar.timestamp,
            instrument=bar.instrument,
            sequence_id=f"synthetic:{bar.instrument.symbol}:{bar.timestamp.isoformat()}",
            bids=tuple(bids),
            asks=tuple(asks),
            metadata={
                "source": "synthetic_depth_proxy",
                "proxy_mid_price": f"{mid_price:.8f}",
                "proxy_spread_bps": f"{spread_bps:.8f}",
                "proxy_liquidity_score": f"{liquidity_score:.8f}",
                "signed_quantity_imbalance": f"{imbalance:.8f}",
                "trade_count": str(int(trade_summary["trade_count"])),
                "trade_price_dispersion": f"{dispersion:.8f}",
                "total_notional": f"{trade_summary['total_notional']:.8f}",
            },
        )


class SyntheticDepthDatasetService:
    def __init__(
        self,
        store: ParquetMarketDataStore,
        catalog: DataCatalog,
        builder: SyntheticDepthSnapshotBuilder | None = None,
    ) -> None:
        self._store = store
        self._catalog = catalog
        self._builder = builder or SyntheticDepthSnapshotBuilder()

    def materialize_binance_range(
        self,
        bars: Sequence[Bar],
        trades: Sequence[Trade],
        *,
        interval: str,
        storage_path: Path,
        market: str = "spot",
        metadata: Mapping[str, str] | None = None,
    ) -> SyntheticDepthMaterializationResult:
        snapshots = self._builder.build(bars, trades)
        if not snapshots:
            return SyntheticDepthMaterializationResult(snapshots=(), datasets=())

        grouped: dict[tuple[str, str], list[OrderBookSnapshot]] = defaultdict(list)
        for snapshot in snapshots:
            grouped[(snapshot.instrument.symbol, snapshot.timestamp.date().isoformat())].append(snapshot)

        datasets: list[DatasetRef] = []
        schema = schema_for_kind(DatasetKind.ORDER_BOOK_SNAPSHOT).columns
        for (symbol, version), bucket in sorted(grouped.items()):
            dataset = DatasetRef(
                name=binance_synthetic_depth_dataset_name(symbol, interval=interval, market=market),
                version=version,
                data_kind=DatasetKind.ORDER_BOOK_SNAPSHOT,
                asset_class=bucket[0].instrument.asset_class,
                location=storage_path / binance_synthetic_depth_dataset_name(symbol, interval=interval, market=market) / version,
                schema=schema,
                storage_tier=StorageTier.CURATED,
                row_count=sum(len(snapshot.bids) + len(snapshot.asks) for snapshot in bucket),
                partition_columns=("symbol", "date"),
                metadata={
                    "source": "synthetic_depth_proxy",
                    "interval": interval,
                    "market": market,
                    "levels": str(self._builder.config.levels),
                    **{str(key): str(value) for key, value in (metadata or {}).items()},
                },
            )
            self._store.write_order_book_snapshots(dataset, bucket)
            self._catalog.register(dataset)
            datasets.append(dataset)
        return SyntheticDepthMaterializationResult(snapshots=snapshots, datasets=tuple(datasets))


def binance_synthetic_depth_dataset_name(symbol: str, *, interval: str, market: str = "spot") -> str:
    return f"binance.{market}.synthetic_depth.{symbol.lower()}.{interval.lower()}"


def _bucket_trades_by_bar(bars: Sequence[Bar], trades: Sequence[Trade]) -> dict[datetime, Mapping[str, float]]:
    if not bars:
        return {}
    buckets: dict[datetime, list[Trade]] = {bar.timestamp: [] for bar in bars}
    trade_index = 0
    for index, bar in enumerate(bars):
        bucket_end = bars[index + 1].timestamp if index + 1 < len(bars) else bar.timestamp + _infer_bucket_delta(bars)
        while trade_index < len(trades) and trades[trade_index].timestamp < bar.timestamp:
            trade_index += 1
        cursor = trade_index
        while cursor < len(trades) and trades[cursor].timestamp < bucket_end:
            if trades[cursor].timestamp >= bar.timestamp:
                buckets[bar.timestamp].append(trades[cursor])
            cursor += 1
        trade_index = cursor
    return {timestamp: _trade_summary(bucket) for timestamp, bucket in buckets.items()}


def _trade_summary(trades: Sequence[Trade]) -> Mapping[str, float]:
    if not trades:
        return _empty_trade_summary()
    prices = [trade.price for trade in trades]
    buy_quantity = sum(trade.quantity for trade in trades if trade.side is Side.BUY)
    sell_quantity = sum(trade.quantity for trade in trades if trade.side is Side.SELL)
    total_quantity = buy_quantity + sell_quantity
    total_notional = sum(trade.notional for trade in trades)
    return {
        "trade_count": float(len(trades)),
        "total_quantity": total_quantity,
        "total_notional": total_notional,
        "signed_quantity_imbalance": _safe_ratio(buy_quantity - sell_quantity, total_quantity),
        "trade_price_dispersion": _safe_ratio(pstdev(prices) if len(prices) > 1 else 0.0, abs(prices[0]) or 1.0),
    }


def _empty_trade_summary() -> Mapping[str, float]:
    return {
        "trade_count": 0.0,
        "total_quantity": 0.0,
        "total_notional": 0.0,
        "signed_quantity_imbalance": 0.0,
        "trade_price_dispersion": 0.0,
    }


def _infer_bucket_delta(bars: Sequence[Bar]) -> timedelta:
    if len(bars) < 2:
        return timedelta(minutes=1)
    deltas = [
        bars[index + 1].timestamp - bars[index].timestamp
        for index in range(len(bars) - 1)
        if bars[index + 1].timestamp > bars[index].timestamp
    ]
    return min(deltas) if deltas else timedelta(minutes=1)


def _level_weights(levels: int, decay: float) -> tuple[float, ...]:
    raw = [exp(-decay * index) for index in range(max(levels, 1))]
    total = sum(raw) or 1.0
    return tuple(value / total for value in raw)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(float(denominator)) <= 1e-12:
        return 0.0
    return float(numerator) / float(denominator)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
