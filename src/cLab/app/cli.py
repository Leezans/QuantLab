from __future__ import annotations

import argparse

from cLab.pipelines.get_data import download_ticker_price_and_store, get_latest_price


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="clab")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_price = sub.add_parser("price", help="Fetch latest price from Binance public API")
    p_price.add_argument("--symbol", required=True, help="Symbol like BTCUSDT")

    p_fetch = sub.add_parser("fetch-price", help="Fetch price and store to file database")
    p_fetch.add_argument("--symbol", required=True, help="Symbol like BTCUSDT")
    p_fetch.add_argument("--date", default=None, help="YYYY-MM-DD (UTC). Default: today")

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

    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
