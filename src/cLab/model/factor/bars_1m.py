from __future__ import annotations

import pandas as pd


def aggtrades_to_bars_1m(df: pd.DataFrame) -> pd.DataFrame:
    """Build 1m OHLCV bars from aggTrades.

    Input expected columns (from Binance aggTrades JSON):
      - T (ms), p (price str), q (qty str)

    Output:
      - minute (UTC)
      - open, high, low, close
      - volume_base, volume_quote
      - n_trades
    """

    if df.empty:
        return pd.DataFrame(
            columns=[
                "minute",
                "open",
                "high",
                "low",
                "close",
                "volume_base",
                "volume_quote",
                "n_trades",
            ]
        )

    x = df.copy()
    x["trade_time"] = pd.to_datetime(x["T"].astype("int64"), unit="ms", utc=True)
    x["price"] = x["p"].astype("float64")
    x["qty"] = x["q"].astype("float64")
    x["quote"] = x["price"] * x["qty"]
    x["minute"] = x["trade_time"].dt.floor("min")

    g = x.groupby("minute", sort=True)

    out = pd.DataFrame(
        {
            "open": g["price"].first(),
            "high": g["price"].max(),
            "low": g["price"].min(),
            "close": g["price"].last(),
            "volume_base": g["qty"].sum(),
            "volume_quote": g["quote"].sum(),
            "n_trades": g.size(),
        }
    ).reset_index()

    return out
