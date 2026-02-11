from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class BacktestResult:
    equity: pd.Series
    stats: dict


def run_buy_and_hold(bars: pd.DataFrame, *, price_col: str = "close") -> BacktestResult:
    """Minimal baseline backtest: buy at first bar, hold to end."""

    if bars.empty:
        raise ValueError("bars is empty")
    px = bars[price_col].astype("float64")
    equity = px / float(px.iloc[0])
    stats = {
        "start": str(bars.index[0]) if len(bars.index) else None,
        "end": str(bars.index[-1]) if len(bars.index) else None,
        "return": float(equity.iloc[-1] - 1.0),
    }
    return BacktestResult(equity=equity, stats=stats)
