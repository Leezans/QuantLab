from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from statistics import fmean, pstdev

from quantlab.core.models import FeatureVector, Instrument


class FactorNormalization(StrEnum):
    RAW = "raw"
    RANK = "rank"
    ZSCORE = "zscore"


@dataclass(frozen=True, slots=True)
class FactorDefinition:
    name: str
    feature_name: str
    sign: float = 1.0
    normalization: FactorNormalization = FactorNormalization.RAW


@dataclass(frozen=True, slots=True)
class FactorExposure:
    factor_name: str
    as_of: datetime
    instrument: Instrument
    value: float
    feature_name: str
    normalization: FactorNormalization


class CandidateFactorGenerator:
    def __init__(
        self,
        normalizations: Sequence[FactorNormalization] | None = None,
        include_inverse: bool = True,
    ) -> None:
        self._normalizations = tuple(normalizations or (FactorNormalization.RAW, FactorNormalization.RANK, FactorNormalization.ZSCORE))
        self._include_inverse = include_inverse

    def generate(self, feature_names: Sequence[str]) -> tuple[FactorDefinition, ...]:
        definitions: list[FactorDefinition] = []
        for feature_name in sorted(set(feature_names)):
            for normalization in self._normalizations:
                definitions.append(
                    FactorDefinition(
                        name=f"{feature_name}__{normalization.value}",
                        feature_name=feature_name,
                        sign=1.0,
                        normalization=normalization,
                    )
                )
                if self._include_inverse:
                    definitions.append(
                        FactorDefinition(
                            name=f"{feature_name}__{normalization.value}__inv",
                            feature_name=feature_name,
                            sign=-1.0,
                            normalization=normalization,
                        )
                    )
        return tuple(definitions)


class FactorMiner:
    def __init__(self, definitions: Sequence[FactorDefinition]) -> None:
        self._definitions = tuple(definitions)

    @property
    def definitions(self) -> tuple[FactorDefinition, ...]:
        return self._definitions

    def mine(self, features: Sequence[FeatureVector]) -> tuple[FactorExposure, ...]:
        by_timestamp: dict[object, list[FeatureVector]] = defaultdict(list)
        for vector in features:
            by_timestamp[vector.as_of].append(vector)

        exposures: list[FactorExposure] = []
        for as_of, timestamp_vectors in sorted(by_timestamp.items(), key=lambda item: item[0]):
            for definition in self._definitions:
                available = [
                    (vector, float(vector.values[definition.feature_name]))
                    for vector in timestamp_vectors
                    if definition.feature_name in vector.values
                ]
                if not available:
                    continue
                transformed = _normalize_values([value for _, value in available], definition.normalization)
                for (vector, _), normalized_value in zip(available, transformed, strict=True):
                    exposures.append(
                        FactorExposure(
                            factor_name=definition.name,
                            as_of=as_of,
                            instrument=vector.instrument,
                            value=definition.sign * normalized_value,
                            feature_name=definition.feature_name,
                            normalization=definition.normalization,
                        )
                    )
        return tuple(sorted(exposures, key=lambda exposure: (exposure.factor_name, exposure.as_of, exposure.instrument.symbol)))


def collect_feature_names(features: Sequence[FeatureVector]) -> tuple[str, ...]:
    names: set[str] = set()
    for vector in features:
        names.update(vector.values)
    return tuple(sorted(names))


def _normalize_values(values: Sequence[float], normalization: FactorNormalization) -> tuple[float, ...]:
    if normalization is FactorNormalization.RAW:
        return tuple(values)
    if normalization is FactorNormalization.ZSCORE:
        std = pstdev(values) if len(values) > 1 else 0.0
        if std == 0.0:
            return tuple(0.0 for _ in values)
        mean_value = fmean(values)
        return tuple((value - mean_value) / std for value in values)
    ranks = _average_ranks(values)
    if len(values) == 1:
        return (0.0,)
    denominator = len(values) - 1
    return tuple(((rank - 1.0) / denominator) * 2.0 - 1.0 for rank in ranks)


def _average_ranks(values: Sequence[float]) -> tuple[float, ...]:
    order = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(order):
        next_index = index + 1
        while next_index < len(order) and order[next_index][1] == order[index][1]:
            next_index += 1
        average_rank = (index + 1 + next_index) / 2.0
        for position in range(index, next_index):
            ranks[order[position][0]] = average_rank
        index = next_index
    return tuple(ranks)
