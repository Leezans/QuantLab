from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Any
import pandas as pd


@dataclass(frozen=True)
class PipelineResult:
    ok: bool
    detail: dict[str, Any]


class LabService(Protocol):
    def lab_key(self) -> str: ...
    def list_symbols(self) -> list[str]: ...
    def load_timeseries(self, symbol: str, start: str, end: str, freq: str) -> pd.DataFrame: ...
    def run_pipeline_build_features(self, symbol: str, *, date_str: str, max_records: int = 5000) -> dict[str, Any]: ...
    def run_factor_eval(self, symbol: str, *, date_str: str, factor_col: str, horizon: int = 60) -> dict[str, Any]: ...
