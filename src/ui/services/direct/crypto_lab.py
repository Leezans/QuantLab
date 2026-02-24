from __future__ import annotations

from dataclasses import dataclass

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
from cLab.pipelines.get_data import (
    PipelineOptions,
    TradesRangePipeline,
    TradesRangeRequest as CLabTradesRangeRequest,
)
from ui.services.types.common import (
    EnsureFactorsRequest,
    EnsureFactorsResult,
    EnsureKlinesRequest,
    EnsureKlinesResult,
    TaskRef,
    TaskStatus,
)
from ui.services.types.cryptos import TradesRangeRequest, TradesRangeResult
from ui.services.orchestrators.factors import build_factor_cache_path, compute_basic_factors
from ui.services.orchestrators.market_data import (
    iter_dates,
    merge_kline_frames,
    normalize_kline_frame,
    parse_date_yyyy_mm_dd,
)


@dataclass
class CLabService:
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

    def ensure_klines(self, req: EnsureKlinesRequest) -> EnsureKlinesResult:
        symbol = req.symbol.strip().upper()
        start_date = parse_date_yyyy_mm_dd(req.start)
        end_date = parse_date_yyyy_mm_dd(req.end)

        filedb = self._build_filedb(req.style)
        client = BinanceVisionClient()
        market = self._to_market(req.market)

        cached_days = 0
        fetched_days = 0
        failed_days = 0
        parquet_paths: list[str] = []
        errors: list[str] = []

        dates = list(iter_dates(start_date, end_date))
        for day in dates:
            day_str = day.strftime("%Y-%m-%d")
            spec = BinanceFileSpec(
                market=market,
                frequency=Frequency.DAILY,
                dataset=Dataset.KLINES,
                symbol=symbol,
                date=day_str,
                interval=req.interval,
                with_checksum=False,
            )
            try:
                if filedb.parquet_exists(spec):
                    cached_days += 1
                    parquet_paths.append(str(filedb.parquet_path(spec)))
                    continue

                parquet_path = client.download_and_convert(
                    filedb=filedb,
                    spec=spec,
                    fetch_checksum=req.fetch_checksum,
                    verify=req.verify_checksum,
                    compression=req.compression,
                )
                fetched_days += 1
                parquet_paths.append(parquet_path)
            except Exception as exc:
                failed_days += 1
                errors.append(f"{symbol} {day_str}: {type(exc).__name__}: {exc}")
                if req.raise_on_error:
                    raise

        frames: list[pd.DataFrame] = []
        for path in parquet_paths:
            try:
                raw = pd.read_parquet(path)
                normalized = normalize_kline_frame(raw)
                normalized["symbol"] = symbol
                normalized["interval"] = req.interval
                frames.append(normalized)
            except Exception as exc:
                failed_days += 1
                errors.append(f"read parquet failed {path}: {type(exc).__name__}: {exc}")
                if req.raise_on_error:
                    raise

        merged = merge_kline_frames(frames)
        source = self._resolve_source(cached_days=cached_days, fetched_days=fetched_days, failed_days=failed_days)

        return EnsureKlinesResult(
            symbol=symbol,
            interval=req.interval,
            source=source,
            dataframe=merged,
            total_days=len(dates),
            cached_days=cached_days,
            fetched_days=fetched_days,
            failed_days=failed_days,
            parquet_paths=parquet_paths,
            errors=errors,
        )

    def ensure_factors(self, req: EnsureFactorsRequest) -> EnsureFactorsResult:
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

        kline_req = EnsureKlinesRequest(
            symbol=req.symbol,
            start=req.start,
            end=req.end,
            interval=req.interval,
            market=req.market,
            style=req.style,
        )
        kline_result = self.ensure_klines(kline_req)
        factors = compute_basic_factors(kline_result.dataframe)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        factors.to_parquet(cache_path, engine="pyarrow", compression="snappy")

        return EnsureFactorsResult(
            symbol=req.symbol.strip().upper(),
            factor_set=req.factor_set,
            source="computed",
            dataframe=factors,
            cache_path=str(cache_path),
            input_source=kline_result.source,
            errors=list(kline_result.errors),
        )

    def run_trades_range(self, req: TradesRangeRequest) -> TradesRangeResult:
        symbol = req.symbol.strip().upper()
        start_date = parse_date_yyyy_mm_dd(req.start)
        end_date = parse_date_yyyy_mm_dd(req.end)

        pipeline_req = CLabTradesRangeRequest(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            market=self._to_market(req.market),
        )
        pipeline_opt = PipelineOptions(
            layout_style=self._to_style(req.style),
            fetch_checksum=req.fetch_checksum,
            verify_checksum=req.verify_checksum,
            compression=req.compression,
            raise_on_error=req.raise_on_error,
        )

        filedb = self._build_filedb(req.style)
        client = BinanceVisionClient()
        pipeline = TradesRangePipeline(filedb=filedb, client=client, options=pipeline_opt)
        result = pipeline.run(pipeline_req)
        source = self._resolve_source(cached_days=result.skipped, fetched_days=result.ok, failed_days=result.failed)

        return TradesRangeResult(
            symbol=symbol,
            source=source,
            total_days=result.total_days,
            ok=result.ok,
            skipped=result.skipped,
            failed=result.failed,
            parquet_paths=result.parquet_paths,
            errors=result.errors,
        )

    def start_task(self, name: str, payload: dict) -> TaskRef:
        return TaskRef(task_id=f"sync-{name}", status="completed", detail={"mode": "sync", "payload": payload})

    def get_task(self, task_id: str) -> TaskStatus:
        return TaskStatus(task_id=task_id, status="completed", detail={"mode": "sync"})

    def _build_filedb(self, style: str) -> FileDB:
        layout = BinancePathLayout(base_path=db_cfg.BINANCE_DIR, style=self._to_style(style))
        return FileDB(layout=layout)

    def _to_style(self, style: str) -> LayoutStyle:
        return LayoutStyle.HIVE if style == "hive" else LayoutStyle.MIRROR

    def _to_market(self, market: str) -> Market:
        if market == "futures":
            return Market.FUTURES
        return Market.SPOT

    def _resolve_source(self, cached_days: int, fetched_days: int, failed_days: int) -> str:
        if failed_days and fetched_days == 0 and cached_days == 0:
            return "error"
        if fetched_days and cached_days:
            return "mixed"
        if fetched_days:
            return "fetched"
        return "cache"


# Backward-compatible alias.
CryptosLabService = CLabService
