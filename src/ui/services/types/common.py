from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class EnsureKlinesRequest:
    symbol: str
    start: str
    end: str
    interval: str = "1m"
    market: str = "spot"
    style: str = "mirror"
    fetch_checksum: bool = True
    verify_checksum: bool = True
    compression: str = "snappy"
    raise_on_error: bool = False


@dataclass(frozen=True)
class EnsureKlinesResult:
    symbol: str
    interval: str
    source: str
    dataframe: pd.DataFrame
    total_days: int
    cached_days: int
    fetched_days: int
    failed_days: int
    parquet_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class EnsureFactorsRequest:
    symbol: str
    start: str
    end: str
    interval: str = "1m"
    market: str = "spot"
    style: str = "mirror"
    factor_set: str = "basic"


@dataclass(frozen=True)
class EnsureFactorsResult:
    symbol: str
    factor_set: str
    source: str
    dataframe: pd.DataFrame
    cache_path: str
    input_source: str
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TaskRef:
    task_id: str
    status: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskStatus:
    task_id: str
    status: str
    detail: dict[str, Any] = field(default_factory=dict)
