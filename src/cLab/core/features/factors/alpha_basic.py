# src/cLab/core/features/factors/alpha_basic.py
from __future__ import annotations

from typing import List, Mapping

from cLab.core.features.operators import pct_change, rolling_mean, rolling_std, rolling_zscore
from cLab.core.features.factors.registry import SeriesDict, register_factor


def _get_int(params: Mapping[str, object], key: str) -> int:
    v = params.get(key)
    if v is None:
        raise ValueError(f"Missing param '{key}'")
    if not isinstance(v, int):
        raise TypeError(f"Param '{key}' must be int")
    return v


@register_factor(
    name="rolling_mean_return",
    required_fields=("close",),
    param_defaults={"window": 20, "min_periods": 20, "return_periods": 1},
    description="Rolling mean of simple returns based on close series.",
)
def rolling_mean_return(data: SeriesDict, params: Mapping[str, object]) -> List[float]:
    window = _get_int(params, "window")
    min_periods = _get_int(params, "min_periods")
    return_periods = _get_int(params, "return_periods")

    close = data["close"]
    r = pct_change(close, periods=return_periods)
    return rolling_mean(r, window=window, min_periods=min_periods)


@register_factor(
    name="rolling_volatility",
    required_fields=("close",),
    param_defaults={"window": 20, "min_periods": 20, "return_periods": 1, "ddof": 0},
    description="Rolling std of simple returns based on close series.",
)
def rolling_volatility(data: SeriesDict, params: Mapping[str, object]) -> List[float]:
    window = _get_int(params, "window")
    min_periods = _get_int(params, "min_periods")
    return_periods = _get_int(params, "return_periods")
    ddof = _get_int(params, "ddof")

    close = data["close"]
    r = pct_change(close, periods=return_periods)
    return rolling_std(r, window=window, ddof=ddof, min_periods=min_periods)


@register_factor(
    name="volume_zscore",
    required_fields=("volume",),
    param_defaults={"window": 60, "min_periods": 30, "ddof": 0},
    description="Rolling z-score of volume series.",
)
def volume_zscore(data: SeriesDict, params: Mapping[str, object]) -> List[float]:
    window = _get_int(params, "window")
    min_periods = _get_int(params, "min_periods")
    ddof = _get_int(params, "ddof")

    vol = data["volume"]
    return rolling_zscore(vol, window=window, ddof=ddof, min_periods=min_periods)