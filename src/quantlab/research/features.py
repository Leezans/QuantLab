from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from statistics import pstdev

from quantlab.core.models import Bar, FeatureVector

FeatureFunction = Callable[[Sequence[Bar], int], float]


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    name: str
    lookback: int
    compute: FeatureFunction


class FeaturePipeline:
    def __init__(self, definitions: Sequence[FeatureDefinition]) -> None:
        self._definitions = tuple(definitions)

    def build(self, bars: Sequence[Bar]) -> tuple[FeatureVector, ...]:
        by_instrument: dict[str, list[Bar]] = defaultdict(list)
        for bar in sorted(bars, key=lambda value: (value.instrument.symbol, value.timestamp)):
            by_instrument[bar.instrument.symbol].append(bar)

        output: list[FeatureVector] = []
        for instrument_bars in by_instrument.values():
            for index, bar in enumerate(instrument_bars):
                values: dict[str, float] = {}
                for definition in self._definitions:
                    if index + 1 < definition.lookback:
                        continue
                    values[definition.name] = float(definition.compute(instrument_bars, index))
                if values:
                    output.append(FeatureVector(as_of=bar.timestamp, instrument=bar.instrument, values=values))
        return tuple(sorted(output, key=lambda vector: (vector.as_of, vector.instrument.symbol)))


def trailing_return(bars: Sequence[Bar], index: int, lookback: int) -> float:
    start = bars[index - lookback + 1].close
    end = bars[index].close
    return 0.0 if start == 0 else (end / start) - 1.0


def rolling_volatility(bars: Sequence[Bar], index: int, lookback: int) -> float:
    window = bars[index - lookback + 1 : index + 1]
    returns: list[float] = []
    for offset in range(1, len(window)):
        previous_close = window[offset - 1].close
        if previous_close == 0:
            continue
        returns.append((window[offset].close / previous_close) - 1.0)
    return pstdev(returns) if len(returns) > 1 else 0.0


def volume_ratio(bars: Sequence[Bar], index: int, lookback: int) -> float:
    window = bars[index - lookback + 1 : index + 1]
    trailing_average = sum(bar.volume for bar in window) / len(window)
    if trailing_average == 0:
        return 0.0
    return bars[index].volume / trailing_average


def intrabar_range(bars: Sequence[Bar], index: int, lookback: int) -> float:
    window = bars[index - lookback + 1 : index + 1]
    average_close = sum(bar.close for bar in window) / len(window)
    if average_close == 0:
        return 0.0
    average_range = sum((bar.high - bar.low) for bar in window) / len(window)
    return average_range / average_close


def make_trailing_return_feature(name: str | None = None, lookback: int = 5) -> FeatureDefinition:
    feature_name = name or f"return_{lookback}"
    return FeatureDefinition(
        name=feature_name,
        lookback=lookback,
        compute=lambda bars, index: trailing_return(bars, index, lookback=lookback),
    )


def make_rolling_volatility_feature(name: str | None = None, lookback: int = 10) -> FeatureDefinition:
    feature_name = name or f"volatility_{lookback}"
    return FeatureDefinition(
        name=feature_name,
        lookback=lookback,
        compute=lambda bars, index: rolling_volatility(bars, index, lookback=lookback),
    )


def make_volume_ratio_feature(name: str | None = None, lookback: int = 10) -> FeatureDefinition:
    feature_name = name or f"volume_ratio_{lookback}"
    return FeatureDefinition(
        name=feature_name,
        lookback=lookback,
        compute=lambda bars, index: volume_ratio(bars, index, lookback=lookback),
    )


def make_intrabar_range_feature(name: str | None = None, lookback: int = 10) -> FeatureDefinition:
    feature_name = name or f"intrabar_range_{lookback}"
    return FeatureDefinition(
        name=feature_name,
        lookback=lookback,
        compute=lambda bars, index: intrabar_range(bars, index, lookback=lookback),
    )
