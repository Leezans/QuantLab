from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from cLab.app import api as clab_api


@dataclass
class CryptoCLabService:
    _lab_key: str = "crypto"

    def lab_key(self) -> str:
        return self._lab_key

    def list_symbols(self) -> list[str]:
        # Prefer bars if available, else factors.
        syms = clab_api.list_symbols()
        if syms:
            return syms
        # fallback: factors only
        return clab_api.list_symbols(dataset="trade_features_1m")

    def load_timeseries(self, symbol: str, start: str, end: str, freq: str) -> pd.DataFrame:
        # Minimal explorer: load bars_1m partitions by date.
        start_d = date.fromisoformat(start)
        end_d = date.fromisoformat(end)

        days = pd.date_range(start_d, end_d, freq="D")
        dfs: list[pd.DataFrame] = []
        for d in days:
            ds = d.date().isoformat()
            part = clab_api.load_parquet(dataset="bars_1m", symbol=symbol, date=ds)
            if part is not None and not part.empty:
                dfs.append(part)

        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        if "minute" in df.columns:
            df = df.sort_values("minute", kind="mergesort")
            df = df.set_index("minute")
        return df

    def run_pipeline_build_features(self, symbol: str, *, date_str: str, max_records: int = 5000) -> dict[str, Any]:
        # End-to-end for a single day:
        dl = clab_api.download_aggtrades(symbol=symbol, date=date_str, max_records=max_records)
        feats = clab_api.build_factors_1m(symbol=symbol, date=date_str)
        bars = clab_api.build_bars_1m(symbol=symbol, date=date_str)
        return {
            "download": dl,
            "trade_features_1m": feats.__dict__,
            "bars_1m": bars.__dict__,
        }

    def run_factor_eval(self, symbol: str, *, date_str: str, factor_col: str, horizon: int = 60) -> dict[str, Any]:
        return clab_api.factor_eval_1m(symbol=symbol, date=date_str, factor_col=factor_col, horizon=horizon)
