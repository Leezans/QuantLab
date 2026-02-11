from __future__ import annotations

import argparse

from cLab.pipelines.get_data import (
    download_aggtrades_day_and_store,
    download_ticker_price_and_store,
    get_latest_price,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="clab")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_price = sub.add_parser("price", help="Fetch latest price from Binance public API")
    p_price.add_argument("--symbol", required=True, help="Symbol like BTCUSDT")

    p_fetch = sub.add_parser("fetch-price", help="Fetch price and store to file database")
    p_fetch.add_argument("--symbol", required=True, help="Symbol like BTCUSDT")
    p_fetch.add_argument("--date", default=None, help="YYYY-MM-DD (UTC). Default: today")

    p_agg = sub.add_parser("fetch-aggtrades", help="Download aggTrades for a day and store to file database")
    p_agg.add_argument("--symbol", required=True, help="Symbol like BTCUSDT")
    p_agg.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    p_agg.add_argument("--max-records", type=int, default=5000, help="Safety cap")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.cmd == "price":
        r = get_latest_price(args.symbol)
        print(f"symbol={r.symbol} price={r.price}")
        return 0

    if args.cmd == "fetch-price":
        out = download_ticker_price_and_store(args.symbol, date=args.date)
        print(f"out={out['out']}")
        print(f"symbol={out['symbol']} date={out['date']} price={out['price']}")
        return 0

    if args.cmd == "fetch-aggtrades":
        out = download_aggtrades_day_and_store(args.symbol, date=args.date, max_records=args.max_records)
        print(f"out={out['out']}")
        print(f"symbol={out['symbol']} date={out['date']} n={out['n']}")
        return 0

    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
