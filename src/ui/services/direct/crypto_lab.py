from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from cLab.config import db_cfg
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
from ui.services.orchestrators.factors import build_factor_cache_path, compute_basic_factors
from ui.services.orchestrators.market_data import iter_dates, parse_date_yyyy_mm_dd
from ui.services.types.common import (
    EnsureFactorsRequest,
    EnsureFactorsResult,
    EnsureKlinesRequest,
    EnsureKlinesResult,
    TaskRef,
    TaskStatus,
)
from ui.services.types.cryptos import (
    KlinesRequestDTO,
    KlinesResultDTO,
    TradesRangeRequest,
    TradesRangeResult,
    TradesRequestDTO,
    TradesResultDTO,
)

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - fallback for minimal environments
    pq = None


@dataclass
class CLabService:
    """Direct adapter backed by cLab local pipelines for crypto market data."""

    _lab_key: str = "crypto"
    _display_name: str = "cLab / CryptosLab"
    _symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT")

    def lab_key(self) -> str:
        return self._lab_key

    def display_name(self) -> str:
        return self._display_name

    def list_symbols(self) -> list[str]:
        return list(self._symbols)

    def supports_trades_download(self) -> bool:
        return True

    def get_or_create_klines(self, req: KlinesRequestDTO) -> KlinesResultDTO:
        """Get-or-create klines parquet files and return preview rows for UI."""
        symbol = req.symbol.strip().upper()
        market = self._to_market(req.market)

        total_days, ok, skipped, failed, parquet_paths, errors = self._get_or_create_daily_dataset_range(
            symbol=symbol,
            market=market,
            style=req.style,
            start=req.start,
            end=req.end,
            dataset=Dataset.KLINES,
            interval=req.interval,
            fetch_checksum=req.fetch_checksum,
            verify_checksum=req.verify_checksum,
            compression=req.compression,
            raise_on_error=req.raise_on_error,
        )

        preview = self._load_klines_preview(parquet_paths=parquet_paths, preview_rows=req.preview_rows)
        row_count = self._count_rows(parquet_paths)
        source = self._resolve_source(ok=ok, skipped=skipped, failed=failed)

        return KlinesResultDTO(
            symbol=symbol,
            market=market.value,
            interval=req.interval,
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

    def get_or_create_klines_range(self, req: KlinesRequestDTO) -> KlinesResultDTO:
        """Backward-compatible wrapper for older orchestrator names."""
        return self.get_or_create_klines(req)

    def get_or_create_trades(self, req: TradesRequestDTO) -> TradesResultDTO:
        """Get-or-create trades parquet files and return preview rows for UI."""
        symbol = req.symbol.strip().upper()
        market = self._to_market(req.market)

        total_days, ok, skipped, failed, parquet_paths, errors = self._get_or_create_daily_dataset_range(
            symbol=symbol,
            market=market,
            style=req.style,
            start=req.start,
            end=req.end,
            dataset=Dataset.TRADES,
            interval=None,
            fetch_checksum=req.fetch_checksum,
            verify_checksum=req.verify_checksum,
            compression=req.compression,
            raise_on_error=req.raise_on_error,
        )

        preview = self._load_trades_preview(parquet_paths=parquet_paths, preview_rows=req.preview_rows)
        row_count = self._count_rows(parquet_paths)
        source = self._resolve_source(ok=ok, skipped=skipped, failed=failed)

        return TradesResultDTO(
            symbol=symbol,
            market=market.value,
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

    def get_or_create_trades_range(self, req: TradesRequestDTO) -> TradesResultDTO:
        """Backward-compatible wrapper for older orchestrator names."""
        return self.get_or_create_trades(req)

    def ensure_klines(self, req: EnsureKlinesRequest) -> EnsureKlinesResult:
        """Compatibility API for existing lab-service callers."""
        result = self.get_or_create_klines(
            KlinesRequestDTO(
                symbol=req.symbol,
                start=req.start,
                end=req.end,
                interval=req.interval,
                market=req.market,
                style=req.style,
                fetch_checksum=req.fetch_checksum,
                verify_checksum=req.verify_checksum,
                compression=req.compression,
                raise_on_error=req.raise_on_error,
            ),
        )
        return EnsureKlinesResult(
            symbol=result.symbol,
            interval=result.interval,
            source=result.source,
            dataframe=result.preview.copy(),
            total_days=result.total_days,
            cached_days=result.skipped,
            fetched_days=result.ok,
            failed_days=result.failed,
            parquet_paths=result.parquet_paths,
            errors=result.errors,
        )

    def ensure_factors(self, req: EnsureFactorsRequest) -> EnsureFactorsResult:
        """Compute simple factors from local preview klines for now."""
        cache_base = db_cfg.CRYPTOS_DATABASE_PATH / "factors"
        cache_path = build_factor_cache_path(cache_base, self.lab_key(), req)

        if cache_path.exists():
            data = pd.read_parquet(cache_path)
            return EnsureFactorsResult(
                symbol=req.symbol.strip().upper(),
                factor_set=req.factor_set,
                source="cache",
                dataframe=data,
                cache_path=str(cache_path),
                input_source="cache",
                errors=[],
            )

        klines_result = self.get_or_create_klines(
            KlinesRequestDTO(
                symbol=req.symbol,
                start=req.start,
                end=req.end,
                interval=req.interval,
                market=req.market,
                style=req.style,
                preview_rows=10000,
            ),
        )

        factors = compute_basic_factors(klines_result.preview)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        factors.to_parquet(cache_path, engine="pyarrow", compression="snappy")

        return EnsureFactorsResult(
            symbol=req.symbol.strip().upper(),
            factor_set=req.factor_set,
            source="computed",
            dataframe=factors,
            cache_path=str(cache_path),
            input_source=klines_result.source,
            errors=list(klines_result.errors),
        )

    def run_trades_range(self, req: TradesRangeRequest) -> TradesRangeResult:
        """Compatibility API for existing callers that still use TradesRangeRequest."""
        return self.get_or_create_trades(
            TradesRequestDTO(
                symbol=req.symbol,
                start=req.start,
                end=req.end,
                market=req.market,
                style=req.style,
                fetch_checksum=req.fetch_checksum,
                verify_checksum=req.verify_checksum,
                compression=req.compression,
                raise_on_error=req.raise_on_error,
            ),
        )

    def start_task(self, name: str, payload: dict) -> TaskRef:
        return TaskRef(task_id=f"sync-{name}", status="completed", detail={"mode": "sync", "payload": payload})

    def get_task(self, task_id: str) -> TaskStatus:
        return TaskStatus(task_id=task_id, status="completed", detail={"mode": "sync"})

    def _get_or_create_daily_dataset_range(
        self,
        *,
        symbol: str,
        market: Market,
        style: str,
        start: str,
        end: str,
        dataset: Dataset,
        interval: str | None,
        fetch_checksum: bool,
        verify_checksum: bool,
        compression: str,
        raise_on_error: bool,
    ) -> tuple[int, int, int, int, list[str], list[str]]:
        """Cache-first downloader for Binance daily datasets."""
        start_date = parse_date_yyyy_mm_dd(start)
        end_date = parse_date_yyyy_mm_dd(end)
        filedb = self._build_filedb(style)
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
                errors.append(f"{symbol} {day_str}: {type(exc).__name__}: {exc}")
                if raise_on_error:
                    raise

        return len(days), ok, skipped, failed, parquet_paths, errors

    def _load_klines_preview(self, *, parquet_paths: list[str], preview_rows: int) -> pd.DataFrame:
        if preview_rows <= 0 or not parquet_paths:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        remaining = preview_rows
        for path in parquet_paths:
            if remaining <= 0:
                break
            frame = self._read_parquet_head(
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
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")
        merged = merged.dropna(subset=["open", "high", "low", "close"]) if not merged.empty else merged
        merged = merged.set_index("open_time")
        merged.index.name = "timestamp"
        return merged.head(preview_rows)

    def _load_trades_preview(self, *, parquet_paths: list[str], preview_rows: int) -> pd.DataFrame:
        if preview_rows <= 0 or not parquet_paths:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        remaining = preview_rows
        for path in parquet_paths:
            if remaining <= 0:
                break
            frame = self._read_parquet_head(
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
        merged = merged.dropna(subset=["price"]) if not merged.empty else merged
        merged = merged.set_index("timestamp")
        merged.index.name = "timestamp"
        return merged.head(preview_rows)

    def _read_parquet_head(self, path: str, *, columns: list[str], nrows: int) -> pd.DataFrame:
        if nrows <= 0:
            return pd.DataFrame(columns=columns)

        if pq is not None:
            try:
                parquet_file = pq.ParquetFile(path)
                frames: list[pd.DataFrame] = []
                remaining = nrows
                batch_size = min(remaining, 50000)
                for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
                    frame = batch.to_pandas()
                    if frame.empty:
                        continue
                    frames.append(frame)
                    remaining -= len(frame)
                    if remaining <= 0:
                        break
                if frames:
                    return pd.concat(frames, ignore_index=True).head(nrows)
            except Exception:
                pass

        return pd.read_parquet(path, columns=columns).head(nrows)

    def _count_rows(self, parquet_paths: Iterable[str]) -> int:
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

    def _build_filedb(self, style: str) -> FileDB:
        layout = BinancePathLayout(base_path=db_cfg.BINANCE_DIR, style=self._to_style(style))
        return FileDB(layout=layout)

    def _to_style(self, style: str) -> LayoutStyle:
        return LayoutStyle.HIVE if style == "hive" else LayoutStyle.MIRROR

    def _to_market(self, market: str) -> Market:
        if market == "futures":
            return Market.FUTURES
        return Market.SPOT

    def _resolve_source(self, *, ok: int, skipped: int, failed: int) -> str:
        if failed and ok == 0 and skipped == 0:
            return "error"
        if ok and skipped:
            return "mixed"
        if ok:
            return "fetched"
        return "cache"


# Backward-compatible alias.
CryptosLabService = CLabService
