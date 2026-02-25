from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from ui.services.types.cryptos import (
    KlinesRequestDTO,
    KlinesResultDTO,
    TradesRequestDTO,
    TradesResultDTO,
    VolumeProfileDTO,
)


def parse_date_yyyy_mm_dd(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value}. Expected YYYY-MM-DD.") from exc


def iter_dates(start: date, end_inclusive: date):
    if end_inclusive < start:
        raise ValueError(f"end_inclusive < start: {end_inclusive} < {start}")
    cur = start
    while cur <= end_inclusive:
        yield cur
        cur += timedelta(days=1)


def normalize_kline_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    if "open_time" in out.columns:
        ts = pd.to_datetime(out["open_time"], unit="ms", utc=True, errors="coerce")
        out = out.loc[~ts.isna()].copy()
        out["open_time"] = ts.loc[~ts.isna()]
        out = out.set_index("open_time")
    elif not pd.api.types.is_datetime64_any_dtype(out.index):
        out.index = pd.to_datetime(out.index, utc=True, errors="coerce")
        out = out.loc[~out.index.isna()].copy()

    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out


def merge_kline_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, axis=0)
    if pd.api.types.is_datetime64_any_dtype(merged.index):
        merged = merged.sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
    return merged


def get_or_create_klines(req: KlinesRequestDTO) -> KlinesResultDTO:
    """Fetch klines with cache-first behavior through a swappable service adapter."""
    from ui.services.registry import get_market_data_service

    adapter = get_market_data_service(req.lab_key)
    return adapter.get_or_create_klines(req)


def get_or_create_trades(req: TradesRequestDTO) -> TradesResultDTO:
    """Fetch trades with cache-first behavior through a swappable service adapter."""
    from ui.services.registry import get_market_data_service

    adapter = get_market_data_service(req.lab_key)
    return adapter.get_or_create_trades(req)


def compute_volume_profile_from_trades(
    trades_preview_or_reader: pd.DataFrame | TradesResultDTO,
    bins: int = 80,
    volume_type: str = "base",
    normalize: bool = False,
) -> VolumeProfileDTO:
    """Compute price-binned volume profile using trades preview data."""
    trades = _extract_trades_preview_frame(trades_preview_or_reader)
    if trades.empty or "price" not in trades.columns:
        return VolumeProfileDTO(volume_type=volume_type, normalized=normalize)

    prices = pd.to_numeric(trades["price"], errors="coerce")
    valid_mask = prices.notna()
    prices = prices.loc[valid_mask]
    if prices.empty:
        return VolumeProfileDTO(volume_type=volume_type, normalized=normalize)

    if volume_type == "quote" and "quote_quantity" in trades.columns:
        weights = pd.to_numeric(trades.loc[valid_mask, "quote_quantity"], errors="coerce").fillna(0.0)
    elif "quantity" in trades.columns:
        weights = pd.to_numeric(trades.loc[valid_mask, "quantity"], errors="coerce").fillna(0.0)
    else:
        weights = pd.Series(np.ones(len(prices)), index=prices.index)

    hist, edges = np.histogram(
        prices.to_numpy(dtype=float),
        bins=max(1, int(bins)),
        weights=weights.to_numpy(dtype=float),
    )
    centers = (edges[:-1] + edges[1:]) / 2.0
    values = hist.astype(float)

    if normalize and values.sum() > 0:
        values = values / values.sum() * 100.0

    return VolumeProfileDTO(
        bin_centers=centers.tolist(),
        volumes=values.tolist(),
        volume_type=volume_type,
        normalized=normalize,
    )


def _extract_trades_preview_frame(trades_preview_or_reader: pd.DataFrame | TradesResultDTO) -> pd.DataFrame:
    if isinstance(trades_preview_or_reader, pd.DataFrame):
        return trades_preview_or_reader
    return trades_preview_or_reader.preview


# Backward compatible aliases for in-flight callers.
get_or_create_klines_range = get_or_create_klines
get_or_create_trades_range = get_or_create_trades
