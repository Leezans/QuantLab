from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

import numpy as np
import pandas as pd

from cLab.core.data.protocols import EnsureRangeResult, ExperimentStore, FeatureStore, MarketDataStore
from cLab.core.domain.errors import DataNotFoundError, ValidationError
from cLab.core.domain.types import RunRecord
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
class ParquetStore(MarketDataStore, FeatureStore, ExperimentStore):
    binance_dir: Path
    features_dir: Path
    runs_dir: Path

    def __post_init__(self) -> None:
        self.features_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    def ensure_klines_range(
        self,
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
        market_enum = _to_market(market)
        total_days, ok, skipped, failed, parquet_paths, errors = self._ensure_dataset_range(
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
        self,
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
        market_enum = _to_market(market)
        total_days, ok, skipped, failed, parquet_paths, errors = self._ensure_dataset_range(
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

    def compute_volume_profile_from_parquet(
        self,
        parquet_paths: list[str],
        *,
        bins: int,
        volume_type: str,
        normalize: bool,
        start_ts: int | None = None,
        end_ts: int | None = None,
        max_rows: int | None = None,
        batch_size: int = 200_000,
    ) -> tuple[list[float], list[float]]:
        if not parquet_paths:
            return [], []

        profile_bins = max(1, int(bins))
        if max_rows is not None and max_rows <= 0:
            return [], []
        if start_ts is not None and end_ts is not None and end_ts < start_ts:
            return [], []

        needs_timestamp_filter = start_ts is not None or end_ts is not None

        min_price = np.inf
        max_price = -np.inf
        scanned = 0
        for frame in _iter_trade_frames(
            parquet_paths,
            batch_size=batch_size,
            include_timestamp=needs_timestamp_filter,
        ):
            prices, _ = _extract_trade_prices_and_weights(
                frame,
                volume_type=volume_type,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            if prices.size == 0:
                continue
            if max_rows is not None:
                remaining = max_rows - scanned
                if remaining <= 0:
                    break
                prices = prices[:remaining]
            if prices.size == 0:
                continue
            scanned += int(prices.size)
            min_price = min(min_price, float(np.min(prices)))
            max_price = max(max_price, float(np.max(prices)))

        if not np.isfinite(min_price) or not np.isfinite(max_price):
            return [], []

        if min_price == max_price:
            epsilon = max(abs(min_price) * 1e-6, 1e-8)
            min_price -= epsilon
            max_price += epsilon

        hist = np.zeros(profile_bins, dtype=float)
        scanned = 0
        for frame in _iter_trade_frames(
            parquet_paths,
            batch_size=batch_size,
            include_timestamp=needs_timestamp_filter,
        ):
            prices, weights = _extract_trade_prices_and_weights(
                frame,
                volume_type=volume_type,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            if prices.size == 0:
                continue
            if max_rows is not None:
                remaining = max_rows - scanned
                if remaining <= 0:
                    break
                prices = prices[:remaining]
                weights = weights[:remaining]
            if prices.size == 0:
                continue
            scanned += int(prices.size)
            batch_hist, _ = np.histogram(
                prices,
                bins=profile_bins,
                range=(min_price, max_price),
                weights=weights,
            )
            hist += batch_hist.astype(float)

        if normalize and hist.sum() > 0:
            hist = hist / hist.sum() * 100.0

        edges = np.linspace(min_price, max_price, profile_bins + 1, dtype=float)
        centers = (edges[:-1] + edges[1:]) / 2.0
        return centers.tolist(), hist.tolist()

    def load_bars(
        self,
        *,
        symbol: str,
        start: date,
        end: date,
        interval: str,
        market: str,
        style: str,
    ) -> pd.DataFrame:
        result = self.ensure_klines_range(
            symbol=symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            interval=interval,
            market=market,
            style=style,
            preview_rows=0,
            fetch_checksum=True,
            verify_checksum=True,
            compression="snappy",
            raise_on_error=False,
        )
        if not result.parquet_paths:
            raise DataNotFoundError(
                f"No bars found for symbol={symbol.upper()} in [{start.isoformat()}, {end.isoformat()}]",
            )

        frames: list[pd.DataFrame] = []
        for path in result.parquet_paths:
            try:
                frame = pd.read_parquet(
                    path,
                    columns=["open_time", "open", "high", "low", "close", "volume"],
                )
                if not frame.empty:
                    frames.append(frame)
            except Exception:
                continue

        if not frames:
            raise DataNotFoundError(
                f"Parquet files exist but are unreadable for symbol={symbol.upper()}",
            )

        bars = pd.concat(frames, ignore_index=True)
        bars["open_time"] = _to_utc_datetime(bars["open_time"])
        bars = bars.loc[~bars["open_time"].isna()].copy()
        for col in ("open", "high", "low", "close", "volume"):
            bars[col] = pd.to_numeric(bars[col], errors="coerce")
        bars = bars.dropna(subset=["open", "high", "low", "close"])
        bars = bars.rename(columns={"open_time": "timestamp"})
        bars = bars.sort_values("timestamp").reset_index(drop=True)
        bars = bars.loc[
            (bars["timestamp"].dt.date >= start) & (bars["timestamp"].dt.date <= end)
        ].reset_index(drop=True)
        return bars

    def save_features(self, symbol: str, factor_set: Iterable[str], frame: pd.DataFrame) -> str:
        symbol_norm = symbol.strip().upper()
        if not symbol_norm:
            raise ValidationError("symbol is empty")
        if frame.empty:
            raise ValidationError("feature frame is empty")

        factor_key = "-".join(sorted(factor_set)) if factor_set else "none"
        filename = f"{symbol_norm}_{factor_key}.parquet"
        path = self.features_dir / filename
        frame.to_parquet(path, index=False)
        return str(path)

    def save_run(self, record: RunRecord, payload: dict[str, Any]) -> str:
        run_id = record.run_id.strip()
        if not run_id:
            raise ValidationError("run_id is empty")

        path = self.runs_dir / f"{run_id}.json"
        data = dict(payload)
        data["run_id"] = run_id
        data["artifact_path"] = str(path)
        if "created_at" not in data:
            data["created_at"] = record.created_at.isoformat()
        path.write_text(
            json.dumps(data, ensure_ascii=True, indent=2, default=_json_default),
            encoding="utf-8",
        )
        return str(path)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        path = self.runs_dir / f"{run_id}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        files = sorted(
            self.runs_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        items: list[dict[str, Any]] = []
        for path in files:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            items.append(data)
            if len(items) >= limit:
                break
        return items

    def _ensure_dataset_range(
        self,
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
        start_date = _parse_yyyy_mm_dd(start)
        end_date = _parse_yyyy_mm_dd(end)
        latest_available = _latest_daily_archive_date_utc()

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

        filedb = self._build_filedb(style)
        client = BinanceVisionClient()

        ok = 0
        skipped = 0
        failed = 0
        parquet_paths: list[str] = []
        errors: list[str] = []

        days = list(_iter_dates(start_date, end_date))
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

    def _build_filedb(self, style: str) -> FileDB:
        layout = BinancePathLayout(
            base_path=self.binance_dir,
            style=_to_layout_style(style),
        )
        return FileDB(layout=layout)


def _parse_yyyy_mm_dd(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value}. Expected YYYY-MM-DD.") from exc


def _latest_daily_archive_date_utc() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _iter_dates(start: date, end_inclusive: date) -> Iterator[date]:
    if end_inclusive < start:
        raise ValueError(f"end_inclusive < start: {end_inclusive} < {start}")
    cur = start
    while cur <= end_inclusive:
        yield cur
        cur += timedelta(days=1)


def _to_market(value: str) -> Market:
    return Market.FUTURES if value == "futures" else Market.SPOT


def _to_layout_style(value: str) -> LayoutStyle:
    return LayoutStyle.HIVE if value == "hive" else LayoutStyle.MIRROR


def _format_download_error(*, symbol: str, day: str, exc: Exception) -> str:
    text = str(exc)
    if "404" in text:
        return (
            f"{symbol} {day}: remote daily archive not found (Binance Vision zip source). "
            "If this is today/future date, wait until next UTC day."
        )
    return f"{symbol} {day}: {type(exc).__name__}: {exc}"


def _iter_trade_frames(
    parquet_paths: list[str],
    *,
    batch_size: int,
    include_timestamp: bool,
) -> Iterator[pd.DataFrame]:
    columns = ["price", "quantity", "quote_quantity"]
    if include_timestamp:
        columns.append("timestamp")
    if pq is not None:
        for path in parquet_paths:
            try:
                parquet_file = pq.ParquetFile(path)
                for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
                    frame = batch.to_pandas()
                    if not frame.empty:
                        yield frame
                continue
            except Exception:
                pass

            try:
                frame = pd.read_parquet(path, columns=columns)
                if not frame.empty:
                    yield frame
            except Exception:
                continue
        return

    for path in parquet_paths:
        try:
            frame = pd.read_parquet(path, columns=columns)
            if not frame.empty:
                yield frame
        except Exception:
            continue


def _extract_trade_prices_and_weights(
    frame: pd.DataFrame,
    *,
    volume_type: str,
    start_ts: int | None,
    end_ts: int | None,
) -> tuple[np.ndarray, np.ndarray]:
    if "price" not in frame.columns:
        return np.array([], dtype=float), np.array([], dtype=float)

    prices = pd.to_numeric(frame["price"], errors="coerce")
    if volume_type == "quote" and "quote_quantity" in frame.columns:
        weights = pd.to_numeric(frame["quote_quantity"], errors="coerce")
    elif "quantity" in frame.columns:
        weights = pd.to_numeric(frame["quantity"], errors="coerce")
    else:
        weights = pd.Series(np.ones(len(frame), dtype=float), index=frame.index)

    valid = prices.notna() & weights.notna()
    if start_ts is not None or end_ts is not None:
        if "timestamp" not in frame.columns:
            return np.array([], dtype=float), np.array([], dtype=float)
        seconds = _to_epoch_seconds(frame["timestamp"])
        valid &= seconds.notna()
        if start_ts is not None:
            valid &= seconds >= float(start_ts)
        if end_ts is not None:
            valid &= seconds <= float(end_ts)

    if not valid.any():
        return np.array([], dtype=float), np.array([], dtype=float)

    price_values = prices.loc[valid].to_numpy(dtype=float, copy=False)
    weight_values = weights.loc[valid].to_numpy(dtype=float, copy=False)
    finite = np.isfinite(price_values) & np.isfinite(weight_values)
    if not finite.any():
        return np.array([], dtype=float), np.array([], dtype=float)

    price_values = price_values[finite]
    weight_values = np.clip(weight_values[finite], a_min=0.0, a_max=None)
    return price_values, weight_values


def _to_epoch_seconds(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        dt = pd.to_datetime(series, utc=True, errors="coerce")
        out = pd.Series(np.nan, index=series.index, dtype=float)
        valid = dt.notna()
        out.loc[valid] = dt.loc[valid].astype("int64") / 1e9
        return out

    numeric = pd.to_numeric(series, errors="coerce")
    non_null = numeric.dropna()
    if non_null.empty:
        return pd.Series(np.nan, index=series.index, dtype=float)

    max_abs = float(non_null.abs().max())
    if max_abs >= 1e17:
        divider = 1e9
    elif max_abs >= 1e14:
        divider = 1e6
    elif max_abs >= 1e11:
        divider = 1e3
    else:
        divider = 1.0
    return numeric / divider


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
    merged["open_time"] = _to_utc_datetime(merged["open_time"])
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
    merged["timestamp"] = _to_utc_datetime(merged["timestamp"])
    merged = merged.loc[~merged["timestamp"].isna()].copy()
    merged = merged.sort_values("timestamp")
    for col in ("price", "quantity", "quote_quantity"):
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["price"])
    merged["time"] = (merged["timestamp"].astype("int64") // 10**9).astype(int)
    return merged.head(preview_rows)


def _to_utc_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, utc=True, errors="coerce")

    numeric = pd.to_numeric(series, errors="coerce")
    non_null = numeric.dropna()
    if non_null.empty:
        return pd.to_datetime(series, utc=True, errors="coerce")

    max_abs = float(non_null.abs().max())
    if max_abs >= 1e17:
        unit = "ns"
    elif max_abs >= 1e14:
        unit = "us"
    elif max_abs >= 1e11:
        unit = "ms"
    else:
        unit = "s"
    return pd.to_datetime(numeric, unit=unit, utc=True, errors="coerce")


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


def _resolve_source(*, ok: int, skipped: int, failed: int) -> str:
    if failed and ok == 0 and skipped == 0:
        return "error"
    if ok and skipped:
        return "mixed"
    if ok:
        return "fetched"
    return "cache"


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)

