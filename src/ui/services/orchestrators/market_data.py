from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd


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

