from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from statistics import fmean, pstdev

from quantlab.core.models import FeatureVector
from quantlab.research.factors import collect_feature_names
from quantlab.research.regime import RegimeObservation, RegimeObservationFrame


class CrossSectionalRegimeObservationBuilder:
    def __init__(
        self,
        feature_names: Sequence[str] | None = None,
        aggregations: Sequence[str] = ("mean", "stdev", "breadth"),
    ) -> None:
        self._feature_names = tuple(feature_names) if feature_names else ()
        self._aggregations = tuple(dict.fromkeys(str(item) for item in aggregations))

    def build_market_frame(
        self,
        features: Sequence[FeatureVector],
        *,
        scope: str = "market",
    ) -> RegimeObservationFrame:
        selected = self._feature_names or collect_feature_names(features)
        by_timestamp: dict[object, list[FeatureVector]] = defaultdict(list)
        for vector in features:
            by_timestamp[vector.as_of].append(vector)

        observations: list[RegimeObservation] = []
        for as_of, vectors in sorted(by_timestamp.items(), key=lambda item: item[0]):
            values: dict[str, float] = {}
            for feature_name in selected:
                available = [
                    float(vector.values[feature_name])
                    for vector in vectors
                    if feature_name in vector.values
                ]
                if not available:
                    continue
                if "mean" in self._aggregations:
                    values[f"{feature_name}__mean"] = fmean(available)
                if "stdev" in self._aggregations:
                    values[f"{feature_name}__stdev"] = pstdev(available) if len(available) > 1 else 0.0
                if "breadth" in self._aggregations:
                    values[f"{feature_name}__breadth"] = sum(1 for value in available if value > 0.0) / len(available)
                if "range" in self._aggregations:
                    values[f"{feature_name}__range"] = max(available) - min(available)
            if values:
                observations.append(RegimeObservation(as_of=as_of, scope=scope, values=values))

        feature_names = tuple(sorted({name for observation in observations for name in observation.values}))
        return RegimeObservationFrame(
            scope=scope,
            observations=tuple(observations),
            feature_names=feature_names,
            metadata={"builder": "cross_sectional"},
        )

    def build_symbol_frames(self, features: Sequence[FeatureVector]) -> tuple[RegimeObservationFrame, ...]:
        selected = self._feature_names or collect_feature_names(features)
        by_symbol: dict[str, list[FeatureVector]] = defaultdict(list)
        for vector in features:
            by_symbol[vector.instrument.symbol].append(vector)

        frames: list[RegimeObservationFrame] = []
        for symbol, vectors in sorted(by_symbol.items()):
            observations = [
                RegimeObservation(
                    as_of=vector.as_of,
                    scope=symbol,
                    values={name: float(vector.values[name]) for name in selected if name in vector.values},
                    metadata={"symbol": symbol},
                )
                for vector in sorted(vectors, key=lambda item: item.as_of)
                if any(name in vector.values for name in selected)
            ]
            if observations:
                frames.append(
                    RegimeObservationFrame(
                        scope=symbol,
                        observations=tuple(observations),
                        feature_names=tuple(sorted({name for observation in observations for name in observation.values})),
                        metadata={"builder": "symbol"},
                    )
                )
        return tuple(frames)
