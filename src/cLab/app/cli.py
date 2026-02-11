from __future__ import annotations

import argparse

from cLab.pipelines.get_data import get_latest_price


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="clab")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_price = sub.add_parser("price", help="Fetch latest price from Binance public API")
    p_price.add_argument("--symbol", required=True, help="Symbol like BTCUSDT")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.cmd == "price":
        r = get_latest_price(args.symbol)
        print(f"symbol={r.symbol} price={r.price}")
        return 0

    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
