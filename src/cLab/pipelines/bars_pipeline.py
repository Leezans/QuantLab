from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from cLab.core.config.db_cfg import DatabaseConfig
from cLab.infra.storage.lab_layout import LabLayout
from cLab.infra.storage.manifest import Manifest, now_ts, write_manifest
from cLab.infra.stores.parquet_store import ParquetStore
from cLab.model.factor.bars_1m import aggtrades_to_bars_1m
from cLab.pipelines.aggtrades_pipeline import load_jsonl


@dataclass(frozen=True)
class BuildBarsResult:
    in_path: str
    out_path: str
    n_rows: int


def build_bars_1m_from_aggtrades_jsonl(*, symbol: str, date: str) -> BuildBarsResult:
    cfg = DatabaseConfig.from_env()
    layout = LabLayout(cfg.file_db_root)

    in_path = layout.file_path("aggtrades_raw", symbol, date, "part-0000.jsonl")
    out_path = layout.file_path("bars_1m", symbol, date, "part-0000.parquet")

    df = load_jsonl(in_path)
    if df.empty:
        raise ValueError(f"No aggtrades data found at {in_path}")

    bars = aggtrades_to_bars_1m(df).sort_values("minute", kind="mergesort").reset_index(drop=True)
    ParquetStore(out_path).write(bars)

    write_manifest(
        layout.manifest_path("bars_1m", symbol, date),
        Manifest(
            dataset="bars_1m",
            symbol=symbol,
            date=date,
            created_at=now_ts(),
            n_rows=int(len(bars)),
            schema={k: str(v) for k, v in bars.dtypes.items()},
        ),
    )

    return BuildBarsResult(in_path=str(in_path), out_path=str(out_path), n_rows=int(len(bars)))
