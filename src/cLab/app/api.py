from __future__ import annotations

"""cLab API surface.

This module exposes a stable function-level API that UIs (Streamlit today,
WebUI later) can call.

We keep it thin: just orchestrate pipelines and return dicts.
"""

from pathlib import Path

import pandas as pd

from cLab.core.config.db_cfg import DatabaseConfig
from cLab.infra.storage.lab_layout import LabLayout
from cLab.core import datasets
from cLab.infra.storage.parquet_store import ParquetStore
from cLab.model.factor.factor_eval import eval_factor
from cLab.pipelines.aggtrades_pipeline import BuildMinuteFactorsResult, build_minute_factors_from_aggtrades_jsonl
from cLab.pipelines.bars_pipeline import BuildBarsResult, build_bars_1m_from_aggtrades_jsonl
from cLab.pipelines.get_data import download_aggtrades_day_and_store, download_ticker_price_and_store


def fetch_ticker_price(*, symbol: str, date: str | None = None) -> dict:
    return download_ticker_price_and_store(symbol, date=date)


def download_aggtrades(*, symbol: str, date: str, max_records: int = 5000) -> dict:
    return download_aggtrades_day_and_store(symbol, date=date, max_records=max_records)


def build_factors_1m(*, symbol: str, date: str) -> BuildMinuteFactorsResult:
    return build_minute_factors_from_aggtrades_jsonl(symbol=symbol, date=date)


def build_bars_1m(*, symbol: str, date: str) -> BuildBarsResult:
    return build_bars_1m_from_aggtrades_jsonl(symbol=symbol, date=date)


def list_symbols(*, dataset: str = datasets.BARS_1M) -> list[str]:
    cfg = DatabaseConfig.from_env()
    root = Path(cfg.file_db_root) / dataset
    if not root.exists():
        return []
    out = [p.name for p in root.iterdir() if p.is_dir()]
    return sorted(out)


def load_parquet(*, dataset: str, symbol: str, date: str) -> pd.DataFrame:
    cfg = DatabaseConfig.from_env()
    layout = LabLayout(cfg.file_db_root)
    p = layout.file_path(dataset, symbol, date, "part-0000.parquet")
    return ParquetStore(p).read()


def factor_eval_1m(*, symbol: str, date: str, factor_col: str, horizon: int = 60) -> dict:
    bars = load_parquet(dataset=datasets.BARS_1M, symbol=symbol, date=date)
    feats = load_parquet(dataset=datasets.TRADE_FEATURES_1M, symbol=symbol, date=date)

    if bars.empty:
        raise ValueError("bars_1m is empty; build bars first")

    df = bars.merge(feats, on="minute", how="left") if not feats.empty else bars
    df = df.sort_values("minute", kind="mergesort").reset_index(drop=True)

    # For eval we need close price column
    if "close" not in df.columns:
        raise ValueError("bars_1m must contain 'close'")

    return eval_factor(df=df, factor_col=factor_col, price_col="close", horizon=horizon, n_quantiles=5)
