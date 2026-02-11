from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from cLab.infra.dataSource.binance import BinancePublicClient


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
