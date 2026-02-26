from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping

import pandas as pd

from cLab.core.data.protocols import FeatureStore, MarketDataStore
from cLab.core.domain.errors import DataNotFoundError, ValidationError
from cLab.core.features.factors import alpha_basic as _alpha_basic  # noqa: F401
from cLab.core.features.factors.registry import get_registry


@dataclass(frozen=True)
class FeatureBuildCommand:
    symbol: str
    start: date
    end: date
    factor_set: list[str]
    interval: str = "1h"
    market: str = "spot"
    style: str = "mirror"
    factor_params: Mapping[str, Mapping[str, object]] | None = None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValidationError("symbol is empty")
        if self.end < self.start:
            raise ValidationError("end < start")
        if not self.factor_set:
            raise ValidationError("factor_set is empty")


@dataclass(frozen=True)
class FeatureBuildResult:
    artifact_path: str
    row_count: int


@dataclass(frozen=True)
class FeaturePipeline:
    market_data_store: MarketDataStore
    feature_store: FeatureStore

    def build(self, command: FeatureBuildCommand) -> FeatureBuildResult:
        bars = self.market_data_store.load_bars(
            symbol=command.symbol,
            start=command.start,
            end=command.end,
            interval=command.interval,
            market=command.market,
            style=command.style,
        )
        if bars.empty:
            raise DataNotFoundError(
                f"No bars loaded for {command.symbol.upper()} [{command.start}, {command.end}]",
            )

        base_data = _to_factor_input(bars)
        registry = get_registry()

        frame = pd.DataFrame({"timestamp": bars["timestamp"]})
        factor_params = command.factor_params or {}

        for factor_name in command.factor_set:
            params = factor_params.get(factor_name, {})
            values = registry.compute(factor_name, data=base_data, params=params, strict_fields=True)
            if len(values) != len(frame):
                raise ValidationError(
                    f"factor output length mismatch for {factor_name}: "
                    f"got={len(values)} expected={len(frame)}",
                )
            frame[factor_name] = values

        artifact_path = self.feature_store.save_features(command.symbol, command.factor_set, frame)
        return FeatureBuildResult(artifact_path=artifact_path, row_count=len(frame))


def _to_factor_input(frame: pd.DataFrame) -> dict[str, list[float]]:
    columns = ("open", "high", "low", "close", "volume")
    out: dict[str, list[float]] = {}
    for col in columns:
        if col in frame.columns:
            out[col] = pd.to_numeric(frame[col], errors="coerce").astype(float).tolist()
    return out

