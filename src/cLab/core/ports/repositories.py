from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import pandas as pd

from cLab.core.domain.types import DateRange, RunRecord


class MarketDataRepository(Protocol):
    def save_bars(self, symbol: str, bars: pd.DataFrame) -> str:
        ...

    def load_bars(self, symbol: str, date_range: DateRange) -> pd.DataFrame:
        ...


class FeatureRepository(Protocol):
    def save_features(self, symbol: str, factor_set: Sequence[str], frame: pd.DataFrame) -> str:
        ...


class RunRepository(Protocol):
    def save_run(self, record: RunRecord, payload: dict[str, object]) -> str:
        ...

    def get_run(self, run_id: str) -> dict[str, object] | None:
        ...

    def list_runs(self, limit: int = 20) -> list[dict[str, object]]:
        ...
