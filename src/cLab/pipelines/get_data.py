# cLab/pipelines/get_data.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Sequence

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


def _parse_yyyy_mm_dd(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format: {s}. Expected YYYY-MM-DD.") from e


def iter_dates(start: date, end_inclusive: date) -> Iterator[date]:
    if end_inclusive < start:
        raise ValueError(f"end_inclusive < start: {end_inclusive} < {start}")
    cur = start
    while cur <= end_inclusive:
        yield cur
        cur += timedelta(days=1)


@dataclass(frozen=True)
class TradesRangeRequest:
    symbol: str
    start_date: date
    end_date: date
    market: Market = Market.SPOT
    frequency: Frequency = Frequency.DAILY
    dataset: Dataset = Dataset.TRADES

    def validate(self) -> None:
        if not self.symbol or not self.symbol.strip():
            raise ValueError("symbol is empty.")
        if self.end_date < self.start_date:
            raise ValueError("end_date must be >= start_date.")
        if self.frequency != Frequency.DAILY:
            raise ValueError("Trades range pipeline currently supports daily only.")
        if self.dataset != Dataset.TRADES:
            raise ValueError("Trades range pipeline is for TRADES dataset only.")


@dataclass(frozen=True)
class PipelineOptions:
    layout_style: LayoutStyle = LayoutStyle.MIRROR
    fetch_checksum: bool = True
    verify_checksum: bool = True
    compression: str = "snappy"
    raise_on_error: bool = False


@dataclass(frozen=True)
class TradesPipelineResult:
    symbol: str
    total_days: int
    ok: int
    skipped: int
    failed: int
    parquet_paths: list[str]
    errors: list[str]


class TradesRangePipeline:
    def __init__(self, filedb: FileDB, client: BinanceVisionClient, options: PipelineOptions):
        self._filedb = filedb
        self._client = client
        self._opt = options

    def run(self, req: TradesRangeRequest) -> TradesPipelineResult:
        req.validate()

        ok = 0
        skipped = 0
        failed = 0
        parquet_paths: list[str] = []
        errors: list[str] = []

        dates = list(iter_dates(req.start_date, req.end_date))
        for d in dates:
            ds = d.strftime("%Y-%m-%d")
            spec = BinanceFileSpec(
                market=req.market,
                frequency=req.frequency,
                dataset=req.dataset,
                symbol=req.symbol,
                date=ds,
                with_checksum=False,
            )

            try:
                if self._filedb.parquet_exists(spec):
                    skipped += 1
                    parquet_paths.append(str(self._filedb.parquet_path(spec)))
                    continue

                parquet_path = self._client.download_and_convert(
                    filedb=self._filedb,
                    spec=spec,
                    fetch_checksum=self._opt.fetch_checksum,
                    verify=self._opt.verify_checksum,
                    compression=self._opt.compression,
                )
                ok += 1
                parquet_paths.append(parquet_path)

            except Exception as e:
                failed += 1
                msg = f"{req.symbol} {ds}: {type(e).__name__}: {e}"
                errors.append(msg)
                if self._opt.raise_on_error:
                    raise

        return TradesPipelineResult(
            symbol=req.symbol,
            total_days=len(dates),
            ok=ok,
            skipped=skipped,
            failed=failed,
            parquet_paths=parquet_paths,
            errors=errors,
        )


def build_default_filedb(style: LayoutStyle = LayoutStyle.MIRROR) -> FileDB:
    layout = BinancePathLayout(base_path=db_cfg.BINANCE_DIR, style=style)
    return FileDB(layout=layout)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="get_data", add_help=True)

    p.add_argument("--symbol", required=True, type=str)
    p.add_argument("--start", required=True, type=str, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, type=str, help="YYYY-MM-DD")

    p.add_argument("--market", default="spot", choices=["spot", "futures"])
    p.add_argument("--style", default="mirror", choices=["mirror", "hive"])
    p.add_argument("--no-checksum", action="store_true")
    p.add_argument("--no-verify", action="store_true")
    p.add_argument("--compression", default="snappy", type=str)
    p.add_argument("--raise-on-error", action="store_true")

    return p.parse_args(argv)


def _to_market(s: str) -> Market:
    return Market.SPOT if s == "spot" else Market.FUTURES


def _to_style(s: str) -> LayoutStyle:
    return LayoutStyle.MIRROR if s == "mirror" else LayoutStyle.HIVE


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    req = TradesRangeRequest(
        symbol=args.symbol.strip(),
        start_date=_parse_yyyy_mm_dd(args.start),
        end_date=_parse_yyyy_mm_dd(args.end),
        market=_to_market(args.market),
        frequency=Frequency.DAILY,
        dataset=Dataset.TRADES,
    )

    opt = PipelineOptions(
        layout_style=_to_style(args.style),
        fetch_checksum=not args.no_checksum,
        verify_checksum=not args.no_verify,
        compression=args.compression,
        raise_on_error=args.raise_on_error,
    )

    filedb = build_default_filedb(style=opt.layout_style)
    client = BinanceVisionClient()

    pipeline = TradesRangePipeline(filedb=filedb, client=client, options=opt)
    result = pipeline.run(req)

    print(
        "TradesRangePipelineResult("
        f"symbol={result.symbol}, total_days={result.total_days}, "
        f"ok={result.ok}, skipped={result.skipped}, failed={result.failed}"
        ")"
    )

    if result.errors:
        print("Errors:")
        for e in result.errors:
            print(f"  - {e}")

    return 0 if result.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())