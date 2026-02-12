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
    def run_pipeline_build_features(self, symbol: str) -> dict[str, Any]: ...
