from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import pandas as pd


@dataclass
class CryptoCLabService:
    _lab_key: str = "crypto"

    def lab_key(self) -> str:
        return self._lab_key

    def list_symbols(self) -> list[str]:
        # TODO: replace with cLab.app.api.list_symbols()
        return ["BTCUSDT", "ETHUSDT"]

    def load_timeseries(self, symbol: str, start: str, end: str, freq: str) -> pd.DataFrame:
        # TODO: replace with cLab.app.api.load_timeseries(symbol, start, end, freq)
        idx = pd.date_range("2024-01-01", periods=200, freq="min")
        df = pd.DataFrame(
            {
                "close": range(len(idx)),
                "symbol": symbol,
                "freq": freq,
            },
            index=idx,
        )
        return df

    def run_pipeline_build_features(self, symbol: str) -> dict[str, Any]:
        # TODO: replace with cLab.app.api.build_features(symbol)
        return {"ok": True, "symbol": symbol, "note": "stub"}
