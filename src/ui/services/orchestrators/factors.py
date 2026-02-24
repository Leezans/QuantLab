from __future__ import annotations

from pathlib import Path

import pandas as pd

from ui.services.types.common import EnsureFactorsRequest


def compute_basic_factors(klines: pd.DataFrame) -> pd.DataFrame:
    if klines.empty or "close" not in klines.columns:
        return pd.DataFrame(index=klines.index)

    close = pd.to_numeric(klines["close"], errors="coerce")
    out = pd.DataFrame(index=klines.index.copy())
    out["close"] = close
    out["ret_1"] = close.pct_change()
    out["ret_5"] = close.pct_change(5)
    out["sma_8"] = close.rolling(8, min_periods=8).mean()
    out["sma_21"] = close.rolling(21, min_periods=21).mean()
    out["ema_21"] = close.ewm(span=21, adjust=False).mean()
    out["volatility_20"] = out["ret_1"].rolling(20, min_periods=20).std()
    out["zscore_20"] = (close - close.rolling(20, min_periods=20).mean()) / close.rolling(
        20,
        min_periods=20,
    ).std()
    return out


def build_factor_cache_path(base_dir: Path, lab_key: str, req: EnsureFactorsRequest) -> Path:
    symbol = req.symbol.strip().upper().replace("/", "_")
    interval = req.interval.replace("/", "_")
    factor_set = req.factor_set.replace("/", "_")
    file_name = f"{symbol}_{interval}_{req.start}_{req.end}_{factor_set}.parquet"
    return base_dir / lab_key / symbol / interval / file_name
