from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Manifest:
    dataset: str
    symbol: str
    date: str
    created_at: float
    n_rows: int
    schema_version: str = "v1"
    schema: dict[str, str] | None = None
    stats: dict[str, Any] | None = None
    source: dict[str, Any] | None = None


def write_manifest(path: str | Path, m: Manifest) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    obj = {
        "dataset": m.dataset,
        "symbol": m.symbol,
        "date": m.date,
        "created_at": float(m.created_at),
        "n_rows": int(m.n_rows),
        "schema_version": m.schema_version,
        "schema": m.schema,
        "stats": m.stats,
        "source": m.source,
    }
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def now_ts() -> float:
    return time.time()
