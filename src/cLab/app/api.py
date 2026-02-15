from __future__ import annotations

"""cLab API surface.

This module exposes a stable function-level API that UIs (Streamlit today,
WebUI later) can call.

We keep it thin: just orchestrate pipelines and return dicts.
"""

from pathlib import Path
import json

import pandas as pd

from cLab.core.config.db_cfg import DatabaseConfig
from cLab.core import datasets
from cLab.infra.storage.lab_layout import LabLayout
from cLab.infra.storage.manifest import read_manifest
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


def list_datasets() -> list[str]:
    cfg = DatabaseConfig.from_env()
    return LabLayout(cfg.file_db_root).list_datasets()


def list_symbols(*, dataset: str = datasets.BARS_1M) -> list[str]:
    cfg = DatabaseConfig.from_env()
    return LabLayout(cfg.file_db_root).list_symbols(dataset)


def list_dates(*, dataset: str, symbol: str) -> list[str]:
    cfg = DatabaseConfig.from_env()
    return LabLayout(cfg.file_db_root).list_dates(dataset, symbol)


def load_parquet(*, dataset: str, symbol: str, date: str) -> pd.DataFrame:
    cfg = DatabaseConfig.from_env()
    layout = LabLayout(cfg.file_db_root)
    p = layout.file_path(dataset, symbol, date, "part-0000.parquet")
    return ParquetStore(p).read()


def load_manifest(*, dataset: str, symbol: str, date: str) -> dict | None:
    cfg = DatabaseConfig.from_env()
    layout = LabLayout(cfg.file_db_root)
    return read_manifest(layout.manifest_path(dataset, symbol, date))


def load_preview(*, dataset: str, symbol: str, date: str, limit: int = 200) -> dict:
    """Best-effort preview loader for UI.

    Returns:
      {"kind": "parquet"|"json"|"jsonl"|"missing", "data": ...}
    """
    cfg = DatabaseConfig.from_env()
    layout = LabLayout(cfg.file_db_root)

    pq_path = layout.file_path(dataset, symbol, date, "part-0000.parquet")
    if pq_path.exists():
        df = ParquetStore(pq_path).read().head(int(limit))
        return {"kind": "parquet", "rows": df.to_dict(orient="records"), "columns": list(df.columns)}

    json_path = layout.file_path(dataset, symbol, date, "price.json")
    if json_path.exists():
        return {"kind": "json", "data": json.loads(json_path.read_text(encoding="utf-8"))}

    jsonl_path = layout.file_path(dataset, symbol, date, "part-0000.jsonl")
    if jsonl_path.exists():
        rows = []
        with jsonl_path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= int(limit):
                    break
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return {"kind": "jsonl", "rows": rows}

    return {"kind": "missing"}


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
