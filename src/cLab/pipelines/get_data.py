from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from cLab.core.config.database import DatabaseConfig
from cLab.infra.dataSource.binance import BinancePublicClient
from cLab.infra.stores.fileDB import FileStore
from cLab.infra.stores.pathlayout import PathLayout


@dataclass(frozen=True)
class GetPriceResult:
    symbol: str
    price: float


def get_latest_price(symbol: str) -> GetPriceResult:
    client = BinancePublicClient()
    price = client.get_ticker_price(symbol)
    return GetPriceResult(symbol=symbol, price=price)


def price_to_dataframe(r: GetPriceResult) -> pd.DataFrame:
    return pd.DataFrame([{"symbol": r.symbol, "price": r.price}])


def download_ticker_price_and_store(symbol: str, *, date: str | None = None) -> dict:
    """Fetch latest ticker price and store it into the file database.

    Storage format: JSON
    Layout: <file_db_root>/ticker_price/<symbol>/<YYYY-MM-DD>/price.json
    """

    cfg = DatabaseConfig.from_env()
    root = cfg.file_db_root

    date = date or datetime.now(tz=timezone.utc).date().isoformat()
    r = get_latest_price(symbol)

    layout = PathLayout(root)
    p = layout.file_path("ticker_price", symbol, date, "price.json")

    store = FileStore(p)
    store.save_json({"symbol": r.symbol, "date": date, "price": r.price})

    return {"out": str(p), "symbol": r.symbol, "date": date, "price": r.price}


def _day_window_utc(date: str) -> tuple[datetime, datetime]:
    d = datetime.fromisoformat(date).date()
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def download_aggtrades_day_and_store(
    symbol: str,
    *,
    date: str,
    max_records: int = 5000,
) -> dict:
    """Download a single day's aggTrades and store to the file database.

    Storage format: JSONL
    Layout: <file_db_root>/aggtrades/<symbol>/<YYYY-MM-DD>/part-0000.jsonl

    Notes:
        - Binance aggTrades returns at most 1000 per request.
        - This is a best-effort downloader for prototyping.
    """

    cfg = DatabaseConfig.from_env()
    root = cfg.file_db_root

    day_start, day_end = _day_window_utc(date)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000) - 1

    client = BinancePublicClient()

    out: list[dict[str, Any]] = []
    from_id: int | None = None

    while len(out) < int(max_records):
        # Binance rejects some param combinations; start with time window,
        # then paginate by fromId only.
        if from_id is None:
            batch = client.agg_trades(
                symbol=symbol,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                limit=1000,
            )
        else:
            batch = client.agg_trades(
                symbol=symbol,
                from_id=from_id,
                limit=1000,
            )
        if not batch:
            break

        # Filter to day window (safety)
        max_ts = None
        for r in batch:
            ts = int(r.get("T"))
            max_ts = ts if max_ts is None else max(max_ts, ts)
            if start_ms <= ts <= end_ms:
                out.append(r)

        last_id = int(batch[-1]["a"])
        from_id = last_id + 1

        # Stop if server returns fewer than limit.
        if len(batch) < 1000:
            break

        # When paginating by fromId only, stop once we have passed the window.
        if max_ts is not None and max_ts > end_ms:
            break

    layout = PathLayout(root)
    p = layout.file_path("aggtrades", symbol, date, "part-0000.jsonl")
    p.parent.mkdir(parents=True, exist_ok=True)

    # Write JSONL atomically
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for r in out:
            f.write(json.dumps(r, ensure_ascii=False))
            f.write("\n")
    tmp.replace(p)

    return {"out": str(p), "symbol": symbol, "date": date, "n": int(len(out))}
