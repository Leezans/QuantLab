from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from cLab.core.config.db_cfg import DatabaseConfig
from cLab.infra.storage.lab_layout import LabLayout
from cLab.infra.storage.manifest import Manifest, now_ts, write_manifest
from cLab.core import datasets
from cLab.infra.storage.parquet_store import ParquetStore
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

    layout = LabLayout(root)

    in_path = layout.file_path(datasets.AGGTRADES_RAW, symbol, date, "part-0000.jsonl")
    out_path = layout.file_path(datasets.TRADE_FEATURES_1M, symbol, date, "part-0000.parquet")

    df = load_jsonl(in_path)
    if df.empty:
        raise ValueError(f"No aggtrades data found at {in_path}")

    factors = aggtrades_to_minute_factors(df)

    # Ensure deterministic ordering
    factors = factors.sort_values("minute", kind="mergesort").reset_index(drop=True)

    ParquetStore(out_path).write(factors)

    write_manifest(
        layout.manifest_path(datasets.TRADE_FEATURES_1M, symbol, date),
        Manifest(
            dataset=datasets.TRADE_FEATURES_1M,
            symbol=symbol,
            date=date,
            created_at=now_ts(),
            n_rows=int(len(factors)),
            schema={k: str(v) for k, v in factors.dtypes.items()},
            source={"input": str(in_path)},
        ),
    )

    return BuildMinuteFactorsResult(in_path=str(in_path), out_path=str(out_path), n_rows=int(len(factors)))
