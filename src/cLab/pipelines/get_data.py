from __future__ import annotations

import argparse
from collections.abc import Sequence

from cLab.config import db_cfg
from cLab.config.settings import load_settings
from cLab.infra.storage import ParquetStore
from cLab.pipelines.data_pipeline import DataPipeline


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="get_data", add_help=True)
    parser.add_argument("--symbol", required=True, type=str)
    parser.add_argument("--start", required=True, type=str, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=str, help="YYYY-MM-DD")
    parser.add_argument("--market", default="spot", choices=["spot", "futures"])
    parser.add_argument("--style", default="mirror", choices=["mirror", "hive"])
    parser.add_argument("--preview-rows", default=100, type=int)
    parser.add_argument("--no-checksum", action="store_true")
    parser.add_argument("--no-verify", action="store_true")
    parser.add_argument("--compression", default="snappy", type=str)
    parser.add_argument("--raise-on-error", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings()
    store = ParquetStore(
        binance_dir=db_cfg.BINANCE_DIR,
        features_dir=settings.storage.features_dir,
        runs_dir=settings.storage.runs_dir,
    )
    pipeline = DataPipeline(market_data_store=store)

    result = pipeline.get_trades(
        symbol=args.symbol.strip().upper(),
        start=args.start,
        end=args.end,
        market=args.market,
        style=args.style,
        preview_rows=max(1, int(args.preview_rows)),
        fetch_checksum=not args.no_checksum,
        verify_checksum=not args.no_verify,
        compression=args.compression,
        raise_on_error=args.raise_on_error,
    )

    print(
        "TradesRangeResult("
        f"symbol={result.symbol}, total_days={result.total_days}, "
        f"ok={result.ok}, skipped={result.skipped}, failed={result.failed}, "
        f"rows={result.row_count}"
        ")",
    )
    if result.errors:
        print("Errors:")
        for err in result.errors:
            print(f"  - {err}")
    return 0 if result.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

