from __future__ import annotations

import pandas as pd


def aggtrades_to_minute_factors(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate Binance spot aggTrades to 1-minute factors.

    Expected input columns (as strings from JSON):
        - T: trade time in ms
        - p: price (string)
        - q: quantity (string)
        - m: is buyer the market maker (bool)

    Output columns:
        - minute (UTC minute timestamp)
        - vwap
        - volume_base, volume_quote
        - buy_base, sell_base, buy_quote, sell_quote
        - buy_sell_imbalance_base
        - n_trades
        - last_price
        - high_price, low_price
    """

    if df.empty:
        return pd.DataFrame(
            columns=[
                "minute",
                "vwap",
                "volume_base",
                "volume_quote",
                "buy_base",
                "sell_base",
                "buy_quote",
                "sell_quote",
                "buy_sell_imbalance_base",
                "n_trades",
                "last_price",
                "high_price",
                "low_price",
            ]
        )

    x = df.copy()
    x["trade_time"] = pd.to_datetime(x["T"].astype("int64"), unit="ms", utc=True)
    x["price"] = x["p"].astype("float64")
    x["qty"] = x["q"].astype("float64")
    x["quote"] = x["price"] * x["qty"]

    # Binance aggTrades field m: is the buyer the market maker
    # If buyer is market maker -> aggressive side is SELL (taker sells)
    x["is_sell"] = x["m"].astype(bool)
    x["is_buy"] = ~x["is_sell"]

    x["minute"] = x["trade_time"].dt.floor("min")

    g = x.groupby("minute", sort=True)

    volume_base = g["qty"].sum()
    volume_quote = g["quote"].sum()

    buy_base = g.apply(lambda d: float(d.loc[d["is_buy"], "qty"].sum()))
    sell_base = g.apply(lambda d: float(d.loc[d["is_sell"], "qty"].sum()))
    buy_quote = g.apply(lambda d: float(d.loc[d["is_buy"], "quote"].sum()))
    sell_quote = g.apply(lambda d: float(d.loc[d["is_sell"], "quote"].sum()))

    vwap = (volume_quote / volume_base).rename("vwap")
    n_trades = g.size().rename("n_trades")
    last_price = g["price"].last().rename("last_price")
    high_price = g["price"].max().rename("high_price")
    low_price = g["price"].min().rename("low_price")

    out = pd.concat(
        [
            vwap,
            volume_base.rename("volume_base"),
            volume_quote.rename("volume_quote"),
            buy_base.rename("buy_base"),
            sell_base.rename("sell_base"),
            buy_quote.rename("buy_quote"),
            sell_quote.rename("sell_quote"),
            n_trades,
            last_price,
            high_price,
            low_price,
        ],
        axis=1,
    ).reset_index()

    out["buy_sell_imbalance_base"] = out["buy_base"] - out["sell_base"]
    return out
