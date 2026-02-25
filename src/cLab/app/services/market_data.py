from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Iterator

import numpy as np
import pandas as pd

from cLab.core.config import db_cfg
from cLab.infra.dataSource.binance_source import BinanceVisionClient
from cLab.infra.storage.fileDB import (
    BinanceFileSpec,
    BinancePathLayout,
    Dataset,
    FileDB,
    Frequency,
    LayoutStyle,
    Market,
)

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pq = None


@dataclass(frozen=True)
class EnsureRangeResult:
    symbol: str
    market: str
    source: str
    total_days: int
    ok: int
    skipped: int
    failed: int
    row_count: int
    parquet_paths: list[str]
    errors: list[str]
    preview: pd.DataFrame


def parse_yyyy_mm_dd(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value}. Expected YYYY-MM-DD.") from exc


def latest_daily_archive_date_utc() -> date:
    """Binance Vision daily archives are typically available up to UTC yesterday."""
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def iter_dates(start: date, end_inclusive: date) -> Iterator[date]:
    if end_inclusive < start:
        raise ValueError(f"end_inclusive < start: {end_inclusive} < {start}")
    cur = start
    while cur <= end_inclusive:
        yield cur
        cur += timedelta(days=1)


def to_market(value: str) -> Market:
    return Market.FUTURES if value == "futures" else Market.SPOT


def to_layout_style(value: str) -> LayoutStyle:
    return LayoutStyle.HIVE if value == "hive" else LayoutStyle.MIRROR


def ensure_klines_range(
    *,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    market: str,
    style: str,
    preview_rows: int,
    fetch_checksum: bool,
    verify_checksum: bool,
    compression: str,
    raise_on_error: bool,
) -> EnsureRangeResult:
    symbol_norm = symbol.strip().upper()
    market_enum = to_market(market)
    total_days, ok, skipped, failed, parquet_paths, errors = _ensure_dataset_range(
        symbol=symbol_norm,
        market=market_enum,
        start=start,
        end=end,
        style=style,
        dataset=Dataset.KLINES,
        interval=interval,
        fetch_checksum=fetch_checksum,
        verify_checksum=verify_checksum,
        compression=compression,
        raise_on_error=raise_on_error,
    )
    preview = _load_klines_preview(parquet_paths, preview_rows)
    row_count = _count_rows(parquet_paths)
    source = _resolve_source(ok=ok, skipped=skipped, failed=failed)
    return EnsureRangeResult(
        symbol=symbol_norm,
        market=market_enum.value,
        source=source,
        total_days=total_days,
        ok=ok,
        skipped=skipped,
        failed=failed,
        row_count=row_count,
        parquet_paths=parquet_paths,
        errors=errors,
        preview=preview,
    )


def ensure_trades_range(
    *,
    symbol: str,
    start: str,
    end: str,
    market: str,
    style: str,
    preview_rows: int,
    fetch_checksum: bool,
    verify_checksum: bool,
    compression: str,
    raise_on_error: bool,
) -> EnsureRangeResult:
    symbol_norm = symbol.strip().upper()
    market_enum = to_market(market)
    total_days, ok, skipped, failed, parquet_paths, errors = _ensure_dataset_range(
        symbol=symbol_norm,
        market=market_enum,
        start=start,
        end=end,
        style=style,
        dataset=Dataset.TRADES,
        interval=None,
        fetch_checksum=fetch_checksum,
        verify_checksum=verify_checksum,
        compression=compression,
        raise_on_error=raise_on_error,
    )
    preview = _load_trades_preview(parquet_paths, preview_rows)
    row_count = _count_rows(parquet_paths)
    source = _resolve_source(ok=ok, skipped=skipped, failed=failed)
    return EnsureRangeResult(
        symbol=symbol_norm,
        market=market_enum.value,
        source=source,
        total_days=total_days,
        ok=ok,
        skipped=skipped,
        failed=failed,
        row_count=row_count,
        parquet_paths=parquet_paths,
        errors=errors,
        preview=preview,
    )


def compute_volume_profile(
    trades_preview: pd.DataFrame,
    *,
    bins: int,
    volume_type: str,
    normalize: bool,
) -> tuple[list[float], list[float]]:
    if trades_preview.empty or "price" not in trades_preview.columns:
        return [], []

    prices = pd.to_numeric(trades_preview["price"], errors="coerce")
    valid_mask = prices.notna()
    prices = prices.loc[valid_mask]
    if prices.empty:
        return [], []

    if volume_type == "quote" and "quote_quantity" in trades_preview.columns:
        weights = pd.to_numeric(
            trades_preview.loc[valid_mask, "quote_quantity"],
            errors="coerce",
        ).fillna(0.0)
    elif "quantity" in trades_preview.columns:
        weights = pd.to_numeric(
            trades_preview.loc[valid_mask, "quantity"],
            errors="coerce",
        ).fillna(0.0)
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

    return centers.tolist(), values.tolist()


def _ensure_dataset_range(
    *,
    symbol: str,
    market: Market,
    start: str,
    end: str,
    style: str,
    dataset: Dataset,
    interval: str | None,
    fetch_checksum: bool,
    verify_checksum: bool,
    compression: str,
    raise_on_error: bool,
) -> tuple[int, int, int, int, list[str], list[str]]:
    start_date = parse_yyyy_mm_dd(start)
    end_date = parse_yyyy_mm_dd(end)
    latest_available = latest_daily_archive_date_utc()

    if start_date > latest_available:
        raise ValueError(
            "Requested range is beyond available daily archives. "
            f"latest_available={latest_available:%Y-%m-%d} (UTC), start={start_date:%Y-%m-%d}.",
        )
    if end_date > latest_available:
        raise ValueError(
            "Requested end date is beyond available daily archives. "
            f"latest_available={latest_available:%Y-%m-%d} (UTC), end={end_date:%Y-%m-%d}. "
            "Binance Vision source files are daily zip archives and are converted to parquet locally.",
        )

    filedb = _build_filedb(style)
    client = BinanceVisionClient()

    ok = 0
    skipped = 0
    failed = 0
    parquet_paths: list[str] = []
    errors: list[str] = []

    days = list(iter_dates(start_date, end_date))
    for day in days:
        day_str = day.strftime("%Y-%m-%d")
        spec = BinanceFileSpec(
            market=market,
            frequency=Frequency.DAILY,
            dataset=dataset,
            symbol=symbol,
            date=day_str,
            interval=interval,
            with_checksum=False,
        )
        try:
            if filedb.parquet_exists(spec):
                skipped += 1
                parquet_paths.append(str(filedb.parquet_path(spec)))
                continue

            parquet_path = client.download_and_convert(
                filedb=filedb,
                spec=spec,
                fetch_checksum=fetch_checksum,
                verify=verify_checksum,
                compression=compression,
            )
            ok += 1
            parquet_paths.append(parquet_path)
        except Exception as exc:
            failed += 1
            errors.append(_format_download_error(symbol=symbol, day=day_str, exc=exc))
            if raise_on_error:
                raise

    return len(days), ok, skipped, failed, parquet_paths, errors


def _format_download_error(*, symbol: str, day: str, exc: Exception) -> str:
    text = str(exc)
    if "404" in text:
        return (
            f"{symbol} {day}: remote daily archive not found (Binance Vision zip source). "
            "If this is today/future date, wait until next UTC day."
        )
    return f"{symbol} {day}: {type(exc).__name__}: {exc}"


def _load_klines_preview(parquet_paths: list[str], preview_rows: int) -> pd.DataFrame:
    if preview_rows <= 0 or not parquet_paths:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    remaining = preview_rows
    for path in parquet_paths:
        if remaining <= 0:
            break
        frame = _read_parquet_head(
            path,
            columns=["open_time", "open", "high", "low", "close", "volume"],
            nrows=remaining,
        )
        if frame.empty:
            continue
        frames.append(frame)
        remaining -= len(frame)

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, axis=0, ignore_index=True)
    merged["open_time"] = pd.to_datetime(merged["open_time"], unit="ms", utc=True, errors="coerce")
    merged = merged.loc[~merged["open_time"].isna()].copy()
    merged = merged.sort_values("open_time")
    for col in ("open", "high", "low", "close", "volume"):
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["open", "high", "low", "close"])
    merged = merged.rename(columns={"open_time": "timestamp"})
    merged["time"] = (merged["timestamp"].astype("int64") // 10**9).astype(int)
    return merged.head(preview_rows)


def _load_trades_preview(parquet_paths: list[str], preview_rows: int) -> pd.DataFrame:
    if preview_rows <= 0 or not parquet_paths:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    remaining = preview_rows
    for path in parquet_paths:
        if remaining <= 0:
            break
        frame = _read_parquet_head(
            path,
            columns=["timestamp", "price", "quantity", "quote_quantity"],
            nrows=remaining,
        )
        if frame.empty:
            continue
        frames.append(frame)
        remaining -= len(frame)

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, axis=0, ignore_index=True)
    merged["timestamp"] = pd.to_datetime(merged["timestamp"], unit="ms", utc=True, errors="coerce")
    merged = merged.loc[~merged["timestamp"].isna()].copy()
    merged = merged.sort_values("timestamp")
    for col in ("price", "quantity", "quote_quantity"):
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["price"])
    merged["time"] = (merged["timestamp"].astype("int64") // 10**9).astype(int)
    return merged.head(preview_rows)


def _read_parquet_head(path: str, *, columns: list[str], nrows: int) -> pd.DataFrame:
    if nrows <= 0:
        return pd.DataFrame(columns=columns)

    if pq is not None:
        try:
            parquet_file = pq.ParquetFile(path)
            chunks: list[pd.DataFrame] = []
            remaining = nrows
            batch_size = min(max(1, remaining), 50_000)
            for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
                frame = batch.to_pandas()
                if frame.empty:
                    continue
                chunks.append(frame)
                remaining -= len(frame)
                if remaining <= 0:
                    break
            if chunks:
                return pd.concat(chunks, ignore_index=True).head(nrows)
        except Exception:
            pass

    return pd.read_parquet(path, columns=columns).head(nrows)


def _count_rows(parquet_paths: Iterable[str]) -> int:
    total = 0
    for path in parquet_paths:
        if pq is not None:
            try:
                total += int(pq.ParquetFile(path).metadata.num_rows)
                continue
            except Exception:
                pass
        try:
            total += int(pd.read_parquet(path).shape[0])
        except Exception:
            continue
    return total


def _build_filedb(style: str) -> FileDB:
    layout = BinancePathLayout(base_path=db_cfg.BINANCE_DIR, style=to_layout_style(style))
    return FileDB(layout=layout)


def _resolve_source(*, ok: int, skipped: int, failed: int) -> str:
    if failed and ok == 0 and skipped == 0:
        return "error"
    if ok and skipped:
        return "mixed"
    if ok:
        return "fetched"
    return "cache"
