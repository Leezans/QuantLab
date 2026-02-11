from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from cLab.core.config.database import DatabaseConfig
from cLab.infra.stores.parquet_store import ParquetStore
from cLab.infra.stores.pathlayout import PathLayout
from cLab.model.factor.aggtrades_1m import aggtrades_to_minute_factors


@dataclass(frozen=True)
class BuildMinuteFactorsResult:
    in_path: str
    out_path: str
    n_rows: int


def load_jsonl(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    rows = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return pd.DataFrame(rows)


def build_minute_factors_from_aggtrades_jsonl(
    *,
    symbol: str,
    date: str,
    file_db_root: str | None = None,
) -> BuildMinuteFactorsResult:
    cfg = DatabaseConfig.from_env()
    root = file_db_root or cfg.file_db_root

    layout = PathLayout(root)

    in_path = layout.file_path("aggtrades", symbol, date, "part-0000.jsonl")
    out_path = layout.file_path("trade_features_1m", symbol, date, "part-0000.parquet")

    df = load_jsonl(in_path)
    if df.empty:
        raise ValueError(f"No aggtrades data found at {in_path}")

    factors = aggtrades_to_minute_factors(df)

    # Ensure deterministic ordering
    factors = factors.sort_values("minute", kind="mergesort").reset_index(drop=True)

    ParquetStore(out_path).write(factors)

    return BuildMinuteFactorsResult(
        in_path=str(in_path),
        out_path=str(out_path),
        n_rows=int(len(factors)),
    )
