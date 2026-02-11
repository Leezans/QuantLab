from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

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
    store.save_json(
        {
            "symbol": r.symbol,
            "date": date,
            "price": r.price,
        }
    )

    return {"out": str(p), "symbol": r.symbol, "date": date, "price": r.price}
