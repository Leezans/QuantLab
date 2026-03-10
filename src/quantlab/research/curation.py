from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import log
from pathlib import Path
from statistics import fmean, pstdev
from typing import Any

from quantlab.core.enums import AssetClass, DatasetKind, StorageTier
from quantlab.core.models import Bar, FeatureVector, Instrument, OrderBookSnapshot, Trade
from quantlab.data.catalog import DataCatalog, DatasetRef


BASE_FEATURE_FRAME_COLUMNS = (
    "timestamp",
    "date",
    "symbol",
    "venue",
    "asset_class",
    "quote_currency",
    "metadata_json",
)


@dataclass(frozen=True, slots=True)
class CuratedFeatureFrame:
    features: tuple[FeatureVector, ...]
    feature_dimensions: Mapping[str, str]
    dataset: DatasetRef | None = None


class BinanceCuratedFeatureBuilder:
    def __init__(self, lookbacks: Sequence[int] = (3, 5, 10)) -> None:
        self._lookbacks = tuple(sorted({int(lookback) for lookback in lookbacks if int(lookback) >= 2}))

    @property
    def feature_dimensions(self) -> Mapping[str, str]:
        dimensions: dict[str, str] = {
            "bar_range": "dimensionless",
            "close_position": "dimensionless",
            "buy_trade_share": "dimensionless",
            "buy_quantity_share": "dimensionless",
            "signed_quantity_imbalance": "dimensionless",
            "signed_notional_imbalance": "dimensionless",
            "signed_trade_count_imbalance": "dimensionless",
            "vwap_gap": "dimensionless",
            "trade_price_dispersion": "dimensionless",
            "max_trade_share": "dimensionless",
            "trade_sign_autocorrelation": "dimensionless",
            "trade_run_imbalance": "dimensionless",
            "vpin_proxy": "dimensionless",
            "notional_concentration": "dimensionless",
            "large_trade_notional_share": "dimensionless",
            "volume_profile_concentration": "dimensionless",
            "volume_profile_tilt": "dimensionless",
            "volume_profile_entropy": "dimensionless",
            "volume_profile_poc_distance": "dimensionless",
            "volume_profile_value_area_width": "dimensionless",
            "orderflow_toxicity": "dimensionless",
            "orderflow_price_pressure": "dimensionless",
            "orderbook_spread_bps": "dimensionless",
            "orderbook_imbalance": "dimensionless",
            "orderbook_depth_imbalance": "dimensionless",
            "orderbook_microprice_gap": "dimensionless",
            "orderbook_pressure": "dimensionless",
            "orderbook_slope": "dimensionless",
            "orderbook_depth_concentration": "dimensionless",
            "orderbook_liquidity_score": "dimensionless",
        }
        for lookback in self._lookbacks:
            dimensions[f"return_{lookback}"] = "dimensionless"
            dimensions[f"realized_volatility_{lookback}"] = "dimensionless"
            dimensions[f"bar_volume_ratio_{lookback}"] = "dimensionless"
            dimensions[f"trade_count_ratio_{lookback}"] = "dimensionless"
            dimensions[f"avg_trade_size_ratio_{lookback}"] = "dimensionless"
            dimensions[f"signed_quantity_imbalance_mean_{lookback}"] = "dimensionless"
            dimensions[f"signed_notional_imbalance_mean_{lookback}"] = "dimensionless"
            dimensions[f"vpin_proxy_mean_{lookback}"] = "dimensionless"
            dimensions[f"trade_sign_autocorrelation_mean_{lookback}"] = "dimensionless"
            dimensions[f"volume_profile_tilt_mean_{lookback}"] = "dimensionless"
        return dimensions

    def build(
        self,
        bars: Sequence[Bar],
        trades: Sequence[Trade],
        order_books: Sequence[OrderBookSnapshot] = (),
    ) -> CuratedFeatureFrame:
        by_symbol_bars: dict[str, list[Bar]] = defaultdict(list)
        by_symbol_trades: dict[str, list[Trade]] = defaultdict(list)
        by_symbol_books: dict[str, list[OrderBookSnapshot]] = defaultdict(list)
        for bar in sorted(bars, key=lambda item: (item.instrument.symbol, item.timestamp)):
            by_symbol_bars[bar.instrument.symbol].append(bar)
        for trade in sorted(trades, key=lambda item: (item.instrument.symbol, item.timestamp, item.trade_id)):
            by_symbol_trades[trade.instrument.symbol].append(trade)
        for snapshot in sorted(order_books, key=lambda item: (item.instrument.symbol, item.timestamp, item.sequence_id)):
            by_symbol_books[snapshot.instrument.symbol].append(snapshot)

        feature_vectors: list[FeatureVector] = []
        for symbol, symbol_bars in sorted(by_symbol_bars.items()):
            trade_buckets = _bucket_trades(symbol_bars, by_symbol_trades.get(symbol, ()))
            book_buckets = _bucket_order_books(symbol_bars, by_symbol_books.get(symbol, ()))
            max_lookback = max(self._lookbacks, default=2)
            if len(symbol_bars) < max_lookback:
                continue
            for index, bar in enumerate(symbol_bars):
                if index + 1 < max_lookback:
                    continue
                trade_metrics = trade_buckets.get(bar.timestamp, _empty_trade_metrics())
                order_book_metrics = _order_book_metrics(book_buckets.get(bar.timestamp))
                vwap_gap = _safe_ratio(trade_metrics["vwap"] - bar.close, bar.close)
                values = {
                    "bar_range": _safe_ratio(bar.high - bar.low, bar.close),
                    "close_position": _close_position(bar),
                    "buy_trade_share": _safe_ratio(trade_metrics["buy_trade_count"], trade_metrics["trade_count"]),
                    "buy_quantity_share": _safe_ratio(trade_metrics["buy_quantity"], trade_metrics["total_quantity"]),
                    "signed_quantity_imbalance": trade_metrics["signed_quantity_imbalance"],
                    "signed_notional_imbalance": trade_metrics["signed_notional_imbalance"],
                    "signed_trade_count_imbalance": trade_metrics["signed_trade_count_imbalance"],
                    "vwap_gap": vwap_gap,
                    "trade_price_dispersion": trade_metrics["trade_price_dispersion"],
                    "max_trade_share": trade_metrics["max_trade_share"],
                    "trade_sign_autocorrelation": trade_metrics["trade_sign_autocorrelation"],
                    "trade_run_imbalance": trade_metrics["trade_run_imbalance"],
                    "vpin_proxy": trade_metrics["vpin_proxy"],
                    "notional_concentration": trade_metrics["notional_concentration"],
                    "large_trade_notional_share": trade_metrics["large_trade_notional_share"],
                    "volume_profile_concentration": trade_metrics["volume_profile_concentration"],
                    "volume_profile_tilt": trade_metrics["volume_profile_tilt"],
                    "volume_profile_entropy": trade_metrics["volume_profile_entropy"],
                    "volume_profile_poc_distance": trade_metrics["volume_profile_poc_distance"],
                    "volume_profile_value_area_width": trade_metrics["volume_profile_value_area_width"],
                    "orderflow_toxicity": abs(
                        trade_metrics["vpin_proxy"] * trade_metrics["signed_quantity_imbalance"]
                    ),
                    "orderflow_price_pressure": trade_metrics["signed_notional_imbalance"] * vwap_gap,
                    **order_book_metrics,
                }
                for lookback in self._lookbacks:
                    values[f"return_{lookback}"] = _trailing_return(symbol_bars, index, lookback)
                    values[f"realized_volatility_{lookback}"] = _realized_volatility(symbol_bars, index, lookback)
                    values[f"bar_volume_ratio_{lookback}"] = _volume_ratio(
                        [candidate.volume for candidate in symbol_bars],
                        index,
                        lookback,
                    )
                    values[f"trade_count_ratio_{lookback}"] = _volume_ratio(
                        [trade_buckets[candidate.timestamp]["trade_count"] for candidate in symbol_bars],
                        index,
                        lookback,
                    )
                    values[f"avg_trade_size_ratio_{lookback}"] = _volume_ratio(
                        [trade_buckets[candidate.timestamp]["avg_trade_size"] for candidate in symbol_bars],
                        index,
                        lookback,
                    )
                    values[f"signed_quantity_imbalance_mean_{lookback}"] = fmean(
                        trade_buckets[candidate.timestamp]["signed_quantity_imbalance"]
                        for candidate in symbol_bars[index - lookback + 1 : index + 1]
                    )
                    values[f"signed_notional_imbalance_mean_{lookback}"] = fmean(
                        trade_buckets[candidate.timestamp]["signed_notional_imbalance"]
                        for candidate in symbol_bars[index - lookback + 1 : index + 1]
                    )
                    values[f"vpin_proxy_mean_{lookback}"] = fmean(
                        trade_buckets[candidate.timestamp]["vpin_proxy"]
                        for candidate in symbol_bars[index - lookback + 1 : index + 1]
                    )
                    values[f"trade_sign_autocorrelation_mean_{lookback}"] = fmean(
                        trade_buckets[candidate.timestamp]["trade_sign_autocorrelation"]
                        for candidate in symbol_bars[index - lookback + 1 : index + 1]
                    )
                    values[f"volume_profile_tilt_mean_{lookback}"] = fmean(
                        trade_buckets[candidate.timestamp]["volume_profile_tilt"]
                        for candidate in symbol_bars[index - lookback + 1 : index + 1]
                    )
                feature_vectors.append(
                    FeatureVector(
                        as_of=bar.timestamp,
                        instrument=bar.instrument,
                        values=values,
                    )
                )
        return CuratedFeatureFrame(
            features=tuple(sorted(feature_vectors, key=lambda item: (item.as_of, item.instrument.symbol))),
            feature_dimensions=self.feature_dimensions,
        )


class ParquetFeatureFrameStore:
    def write(
        self,
        dataset: DatasetRef,
        features: Sequence[FeatureVector],
    ) -> None:
        pa, ds = _require_pyarrow()
        rows = [_feature_vector_to_row(feature) for feature in features]
        dataset.location.mkdir(parents=True, exist_ok=True)
        if rows:
            table = pa.Table.from_pylist(rows)
            ds.write_dataset(
                table,
                base_dir=str(dataset.location),
                format="parquet",
                partitioning=list(dataset.partition_columns) or ["symbol", "date"],
                partitioning_flavor="hive",
                existing_data_behavior="delete_matching",
            )

    def read(self, dataset: DatasetRef) -> tuple[FeatureVector, ...]:
        _, ds = _require_pyarrow()
        if not any(dataset.location.rglob("*.parquet")):
            return ()
        scanner = ds.dataset(str(dataset.location), format="parquet", partitioning="hive")
        rows = scanner.to_table().to_pylist()
        base_columns = set(BASE_FEATURE_FRAME_COLUMNS)
        features: list[FeatureVector] = []
        for row in rows:
            instrument = Instrument(
                symbol=str(row["symbol"]),
                venue=str(row["venue"]),
                asset_class=AssetClass(str(row["asset_class"])),
                quote_currency=str(row.get("quote_currency", "USD")),
            )
            values = {
                str(key): float(value)
                for key, value in row.items()
                if key not in base_columns and value is not None
            }
            features.append(FeatureVector(as_of=row["timestamp"], instrument=instrument, values=values))
        return tuple(sorted(features, key=lambda item: (item.as_of, item.instrument.symbol)))


class CuratedFeatureDatasetService:
    def __init__(self, store: ParquetFeatureFrameStore, catalog: DataCatalog) -> None:
        self._store = store
        self._catalog = catalog

    def persist(
        self,
        frame: CuratedFeatureFrame,
        *,
        dataset_name: str,
        version: str,
        storage_path: Path,
        metadata: Mapping[str, str] | None = None,
    ) -> CuratedFeatureFrame:
        if not frame.features:
            raise ValueError("cannot persist an empty curated feature frame")
        sample = frame.features[0]
        feature_names = tuple(sorted({name for vector in frame.features for name in vector.values}))
        dataset = DatasetRef(
            name=dataset_name,
            version=version,
            data_kind=DatasetKind.FEATURE_FRAME,
            asset_class=sample.instrument.asset_class,
            location=storage_path / dataset_name / version,
            schema=tuple((*BASE_FEATURE_FRAME_COLUMNS, *feature_names)),
            storage_tier=StorageTier.CURATED,
            row_count=len(frame.features),
            partition_columns=("symbol", "date"),
            metadata={
                "feature_names": ",".join(feature_names),
                "feature_dimensions_json": json.dumps(dict(frame.feature_dimensions), sort_keys=True),
                **{str(key): str(value) for key, value in (metadata or {}).items()},
            },
        )
        self._store.write(dataset, frame.features)
        self._catalog.register(dataset)
        return CuratedFeatureFrame(
            features=frame.features,
            feature_dimensions=frame.feature_dimensions,
            dataset=dataset,
        )


def _feature_vector_to_row(feature: FeatureVector) -> dict[str, Any]:
    return {
        "timestamp": feature.as_of,
        "date": feature.as_of.date().isoformat(),
        "symbol": feature.instrument.symbol,
        "venue": feature.instrument.venue,
        "asset_class": feature.instrument.asset_class.value,
        "quote_currency": feature.instrument.quote_currency,
        "metadata_json": "{}",
        **{key: float(value) for key, value in feature.values.items()},
    }


def _bucket_trades(bars: Sequence[Bar], trades: Sequence[Trade]) -> dict[datetime, dict[str, float]]:
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
    return {timestamp: _trade_metrics(bucket) for timestamp, bucket in buckets.items()}


def _bucket_order_books(
    bars: Sequence[Bar],
    order_books: Sequence[OrderBookSnapshot],
) -> dict[datetime, OrderBookSnapshot | None]:
    if not bars:
        return {}
    buckets: dict[datetime, OrderBookSnapshot | None] = {bar.timestamp: None for bar in bars}
    snapshot_index = 0
    for index, bar in enumerate(bars):
        bucket_end = bars[index + 1].timestamp if index + 1 < len(bars) else bar.timestamp + _infer_bucket_delta(bars)
        latest: OrderBookSnapshot | None = None
        while snapshot_index < len(order_books) and order_books[snapshot_index].timestamp < bar.timestamp:
            snapshot_index += 1
        cursor = snapshot_index
        while cursor < len(order_books) and order_books[cursor].timestamp < bucket_end:
            if order_books[cursor].timestamp >= bar.timestamp:
                latest = order_books[cursor]
            cursor += 1
        buckets[bar.timestamp] = latest
        snapshot_index = cursor
    return buckets


def _trade_metrics(trades: Sequence[Trade]) -> dict[str, float]:
    if not trades:
        return _empty_trade_metrics()
    trade_count = len(trades)
    buy_trades = [trade for trade in trades if str(trade.side or "") == "buy"]
    sell_trades = [trade for trade in trades if str(trade.side or "") == "sell"]
    total_quantity = sum(trade.quantity for trade in trades)
    buy_quantity = sum(trade.quantity for trade in buy_trades)
    sell_quantity = sum(trade.quantity for trade in sell_trades)
    total_notional = sum(trade.notional for trade in trades)
    buy_notional = sum(trade.notional for trade in buy_trades)
    sell_notional = sum(trade.notional for trade in sell_trades)
    prices = [trade.price for trade in trades]
    signs = [1.0 if str(trade.side or "") == "buy" else -1.0 for trade in trades]
    notionals = [trade.notional for trade in trades]
    vwap = _safe_ratio(total_notional, total_quantity)
    return {
        "trade_count": float(trade_count),
        "buy_trade_count": float(len(buy_trades)),
        "sell_trade_count": float(len(sell_trades)),
        "total_quantity": total_quantity,
        "buy_quantity": buy_quantity,
        "sell_quantity": sell_quantity,
        "total_notional": total_notional,
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "avg_trade_size": _safe_ratio(total_quantity, trade_count),
        "vwap": vwap,
        "signed_quantity_imbalance": _safe_ratio(buy_quantity - sell_quantity, total_quantity),
        "signed_notional_imbalance": _safe_ratio(buy_notional - sell_notional, total_notional),
        "signed_trade_count_imbalance": _safe_ratio(len(buy_trades) - len(sell_trades), trade_count),
        "trade_price_dispersion": _safe_ratio(pstdev(prices) if len(prices) > 1 else 0.0, abs(vwap) or 1.0),
        "max_trade_share": _safe_ratio(max(trade.quantity for trade in trades), total_quantity),
        "trade_sign_autocorrelation": _lag_one_autocorrelation(signs),
        "trade_run_imbalance": _trade_run_imbalance(signs),
        "vpin_proxy": _volume_bucket_imbalance(trades, bucket_count=12),
        "notional_concentration": _share_concentration(notionals),
        "large_trade_notional_share": _tail_share(sorted(notionals), total_notional),
        "volume_profile_concentration": _volume_profile_concentration(prices, notionals),
        "volume_profile_tilt": _volume_profile_tilt(prices, notionals),
        "volume_profile_entropy": _volume_profile_entropy(prices, notionals),
        "volume_profile_poc_distance": _volume_profile_poc_distance(prices, notionals),
        "volume_profile_value_area_width": _volume_profile_value_area_width(prices, notionals),
    }


def _empty_trade_metrics() -> dict[str, float]:
    return {
        "trade_count": 0.0,
        "buy_trade_count": 0.0,
        "sell_trade_count": 0.0,
        "total_quantity": 0.0,
        "buy_quantity": 0.0,
        "sell_quantity": 0.0,
        "total_notional": 0.0,
        "buy_notional": 0.0,
        "sell_notional": 0.0,
        "avg_trade_size": 0.0,
        "vwap": 0.0,
        "signed_quantity_imbalance": 0.0,
        "signed_notional_imbalance": 0.0,
        "signed_trade_count_imbalance": 0.0,
        "trade_price_dispersion": 0.0,
        "max_trade_share": 0.0,
        "trade_sign_autocorrelation": 0.0,
        "trade_run_imbalance": 0.0,
        "vpin_proxy": 0.0,
        "notional_concentration": 0.0,
        "large_trade_notional_share": 0.0,
        "volume_profile_concentration": 0.0,
        "volume_profile_tilt": 0.0,
        "volume_profile_entropy": 0.0,
        "volume_profile_poc_distance": 0.0,
        "volume_profile_value_area_width": 0.0,
    }


def _order_book_metrics(snapshot: OrderBookSnapshot | None) -> dict[str, float]:
    if snapshot is None or not snapshot.bids or not snapshot.asks:
        return {
            "orderbook_spread_bps": 0.0,
            "orderbook_imbalance": 0.0,
            "orderbook_depth_imbalance": 0.0,
            "orderbook_microprice_gap": 0.0,
            "orderbook_pressure": 0.0,
            "orderbook_slope": 0.0,
            "orderbook_depth_concentration": 0.0,
            "orderbook_liquidity_score": 0.0,
        }
    best_bid = snapshot.bids[0]
    best_ask = snapshot.asks[0]
    mid = (best_bid.price + best_ask.price) / 2.0
    top_depth = best_bid.quantity + best_ask.quantity
    bid_depth = sum(level.quantity for level in snapshot.bids[:3])
    ask_depth = sum(level.quantity for level in snapshot.asks[:3])
    microprice = _microprice(best_bid.price, best_bid.quantity, best_ask.price, best_ask.quantity)
    pressure = _order_book_pressure(snapshot, mid)
    total_depth = sum(level.quantity for level in snapshot.bids[:5]) + sum(level.quantity for level in snapshot.asks[:5])
    top_level_depth = top_depth
    spread_bps = _safe_ratio(best_ask.price - best_bid.price, mid) * 10_000.0
    liquidity_score = _safe_ratio(
        sum(level.price * level.quantity for level in snapshot.bids[:3])
        + sum(level.price * level.quantity for level in snapshot.asks[:3]),
        max(spread_bps, 1e-9),
    )
    return {
        "orderbook_spread_bps": spread_bps,
        "orderbook_imbalance": _safe_ratio(best_bid.quantity - best_ask.quantity, top_depth),
        "orderbook_depth_imbalance": _safe_ratio(bid_depth - ask_depth, bid_depth + ask_depth),
        "orderbook_microprice_gap": _safe_ratio(microprice - mid, mid),
        "orderbook_pressure": pressure,
        "orderbook_slope": _order_book_slope(snapshot, mid),
        "orderbook_depth_concentration": _safe_ratio(top_level_depth, total_depth),
        "orderbook_liquidity_score": liquidity_score or float(snapshot.metadata.get("proxy_liquidity_score", 0.0)),
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


def _trailing_return(bars: Sequence[Bar], index: int, lookback: int) -> float:
    start = bars[index - lookback + 1].close
    end = bars[index].close
    return _safe_ratio(end - start, start)


def _realized_volatility(bars: Sequence[Bar], index: int, lookback: int) -> float:
    window = bars[index - lookback + 1 : index + 1]
    returns = [
        _safe_ratio(window[offset].close - window[offset - 1].close, window[offset - 1].close)
        for offset in range(1, len(window))
    ]
    return pstdev(returns) if len(returns) > 1 else 0.0


def _volume_ratio(values: Sequence[float], index: int, lookback: int) -> float:
    window = values[index - lookback + 1 : index + 1]
    trailing_mean = fmean(window) if window else 0.0
    return _safe_ratio(values[index], trailing_mean)


def _close_position(bar: Bar) -> float:
    bar_range = bar.high - bar.low
    if bar_range <= 0:
        return 0.0
    return ((bar.close - bar.low) / bar_range) - 0.5


def _safe_ratio(numerator: float, denominator: float | int) -> float:
    denominator_value = float(denominator)
    if abs(denominator_value) <= 1e-12:
        return 0.0
    return float(numerator) / denominator_value


def _lag_one_autocorrelation(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    left = values[:-1]
    right = values[1:]
    left_mean = fmean(left)
    right_mean = fmean(right)
    left_centered = [value - left_mean for value in left]
    right_centered = [value - right_mean for value in right]
    left_scale = sum(value * value for value in left_centered) ** 0.5
    right_scale = sum(value * value for value in right_centered) ** 0.5
    if left_scale <= 1e-12 or right_scale <= 1e-12:
        return 0.0
    covariance = sum(left_value * right_value for left_value, right_value in zip(left_centered, right_centered, strict=True))
    return covariance / (left_scale * right_scale)


def _trade_run_imbalance(signs: Sequence[float]) -> float:
    if not signs:
        return 0.0
    max_buy_run = 0
    max_sell_run = 0
    buy_run = 0
    sell_run = 0
    for sign in signs:
        if sign > 0:
            buy_run += 1
            sell_run = 0
        else:
            sell_run += 1
            buy_run = 0
        max_buy_run = max(max_buy_run, buy_run)
        max_sell_run = max(max_sell_run, sell_run)
    return _safe_ratio(max_buy_run - max_sell_run, len(signs))


def _volume_bucket_imbalance(trades: Sequence[Trade], bucket_count: int) -> float:
    if not trades:
        return 0.0
    total_quantity = sum(trade.quantity for trade in trades)
    if total_quantity <= 1e-12:
        return 0.0
    bucket_volume = total_quantity / max(bucket_count, 1)
    imbalances: list[float] = []
    signed_accumulator = 0.0
    remaining_bucket = bucket_volume
    for trade in trades:
        remaining_trade = trade.quantity
        signed_direction = 1.0 if str(trade.side or "") == "buy" else -1.0
        while remaining_trade > 1e-12:
            slice_volume = min(remaining_trade, remaining_bucket)
            signed_accumulator += signed_direction * slice_volume
            remaining_trade -= slice_volume
            remaining_bucket -= slice_volume
            if remaining_bucket <= 1e-12:
                imbalances.append(abs(signed_accumulator) / bucket_volume)
                signed_accumulator = 0.0
                remaining_bucket = bucket_volume
    if remaining_bucket < bucket_volume and bucket_volume > 0:
        partial_volume = bucket_volume - remaining_bucket
        imbalances.append(abs(signed_accumulator) / partial_volume if partial_volume > 1e-12 else 0.0)
    return fmean(imbalances) if imbalances else 0.0


def _share_concentration(values: Sequence[float]) -> float:
    total = sum(values)
    if total <= 1e-12:
        return 0.0
    return sum((value / total) ** 2 for value in values)


def _tail_share(sorted_values: Sequence[float], total: float) -> float:
    if not sorted_values or total <= 1e-12:
        return 0.0
    tail_count = max(1, len(sorted_values) // 5)
    return sum(sorted_values[-tail_count:]) / total


def _volume_profile_concentration(prices: Sequence[float], weights: Sequence[float], bins: int = 5) -> float:
    profile = _volume_profile_bins(prices, weights, bins=bins)
    return _share_concentration(profile)


def _volume_profile_tilt(prices: Sequence[float], weights: Sequence[float], bins: int = 5) -> float:
    profile = _volume_profile_bins(prices, weights, bins=bins)
    if not profile:
        return 0.0
    midpoint = len(profile) // 2
    lower = sum(profile[:midpoint])
    upper = sum(profile[midpoint + (0 if len(profile) % 2 == 0 else 1) :])
    return _safe_ratio(upper - lower, sum(profile))


def _volume_profile_entropy(prices: Sequence[float], weights: Sequence[float], bins: int = 5) -> float:
    profile = _volume_profile_bins(prices, weights, bins=bins)
    total = sum(profile)
    if total <= 1e-12 or len(profile) <= 1:
        return 0.0
    probabilities = [weight / total for weight in profile if weight > 1e-12]
    if not probabilities:
        return 0.0
    entropy = -sum(probability * log(probability) for probability in probabilities)
    return _safe_ratio(entropy, log(len(profile)))


def _volume_profile_poc_distance(prices: Sequence[float], weights: Sequence[float], bins: int = 5) -> float:
    profile = _volume_profile_bins(prices, weights, bins=bins)
    if not profile or not prices:
        return 0.0
    low = min(prices)
    high = max(prices)
    if high - low <= 1e-12:
        return 0.0
    poc_index = max(range(len(profile)), key=lambda index: profile[index])
    bin_width = (high - low) / max(len(profile), 1)
    poc_center = low + ((poc_index + 0.5) * bin_width)
    reference_price = prices[-1]
    return _safe_ratio(poc_center - reference_price, reference_price)


def _volume_profile_value_area_width(prices: Sequence[float], weights: Sequence[float], bins: int = 5, coverage: float = 0.7) -> float:
    profile = _volume_profile_bins(prices, weights, bins=bins)
    if not profile or not prices:
        return 0.0
    low = min(prices)
    high = max(prices)
    total = sum(profile)
    if high - low <= 1e-12 or total <= 1e-12:
        return 0.0
    left = right = max(range(len(profile)), key=lambda index: profile[index])
    covered = profile[left]
    while covered / total < coverage and (left > 0 or right < len(profile) - 1):
        left_candidate = profile[left - 1] if left > 0 else -1.0
        right_candidate = profile[right + 1] if right < len(profile) - 1 else -1.0
        if right_candidate >= left_candidate and right < len(profile) - 1:
            right += 1
            covered += profile[right]
        elif left > 0:
            left -= 1
            covered += profile[left]
        else:
            break
    bin_width = (high - low) / max(len(profile), 1)
    value_area_width = (right - left + 1) * bin_width
    return _safe_ratio(value_area_width, prices[-1])


def _volume_profile_bins(prices: Sequence[float], weights: Sequence[float], bins: int) -> list[float]:
    if not prices or not weights or len(prices) != len(weights):
        return []
    low = min(prices)
    high = max(prices)
    if high - low <= 1e-12:
        return [sum(weights)]
    bin_weights = [0.0 for _ in range(max(bins, 1))]
    for price, weight in zip(prices, weights, strict=True):
        scaled = (price - low) / (high - low)
        index = min(len(bin_weights) - 1, max(0, int(scaled * len(bin_weights))))
        bin_weights[index] += weight
    return bin_weights


def _microprice(best_bid: float, bid_size: float, best_ask: float, ask_size: float) -> float:
    denominator = bid_size + ask_size
    if denominator <= 1e-12:
        return (best_bid + best_ask) / 2.0
    return ((best_bid * ask_size) + (best_ask * bid_size)) / denominator


def _order_book_pressure(snapshot: OrderBookSnapshot, mid: float) -> float:
    bid_pressure = sum(
        level.quantity / max(_safe_ratio(mid - level.price, mid), 1e-9)
        for level in snapshot.bids[:5]
    )
    ask_pressure = sum(
        level.quantity / max(_safe_ratio(level.price - mid, mid), 1e-9)
        for level in snapshot.asks[:5]
    )
    return _safe_ratio(bid_pressure - ask_pressure, bid_pressure + ask_pressure)


def _order_book_slope(snapshot: OrderBookSnapshot, mid: float) -> float:
    bid_distances = [
        _safe_ratio(mid - level.price, mid)
        for level in snapshot.bids[:5]
    ]
    ask_distances = [
        _safe_ratio(level.price - mid, mid)
        for level in snapshot.asks[:5]
    ]
    if not bid_distances or not ask_distances:
        return 0.0
    bid_slope = _safe_ratio(sum(bid_distances), len(bid_distances))
    ask_slope = _safe_ratio(sum(ask_distances), len(ask_distances))
    return ask_slope - bid_slope


def _require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.dataset as ds
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyarrow is required for curated feature datasets. Install project dependencies first.") from exc
    return pa, ds
