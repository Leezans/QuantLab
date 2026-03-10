from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

from quantlab.research.factor_evaluation import FactorSummary
from quantlab.research.factors import FactorExposure, FactorNormalization


@dataclass(frozen=True, slots=True)
class FactorWeight:
    factor_name: str
    weight: float
    source_metric: str


class EqualWeightFactorCombiner:
    def combine(
        self,
        exposures: Sequence[FactorExposure],
        summaries: Sequence[FactorSummary],
        *,
        composite_name: str = "composite.equal_weight",
        top_n: int | None = None,
    ) -> tuple[tuple[FactorExposure, ...], tuple[FactorWeight, ...]]:
        eligible = _select_summary_names(summaries, top_n=top_n)
        if not eligible:
            return (), ()
        weights = tuple(
            FactorWeight(factor_name=name, weight=1.0 / len(eligible), source_metric="equal_weight")
            for name in eligible
        )
        return _combine_exposures(exposures, weights, composite_name=composite_name), weights


class ICWeightFactorCombiner:
    def combine(
        self,
        exposures: Sequence[FactorExposure],
        summaries: Sequence[FactorSummary],
        *,
        composite_name: str = "composite.ic_weighted",
        top_n: int | None = None,
    ) -> tuple[tuple[FactorExposure, ...], tuple[FactorWeight, ...]]:
        ranked = [
            summary
            for summary in sorted(summaries, key=lambda item: abs(item.mean_ic or 0.0), reverse=True)
            if summary.mean_ic is not None
        ]
        if top_n is not None:
            ranked = ranked[:top_n]
        raw_weights = [summary.mean_ic or 0.0 for summary in ranked]
        denominator = sum(abs(weight) for weight in raw_weights)
        if denominator == 0.0:
            return EqualWeightFactorCombiner().combine(exposures, ranked, composite_name=composite_name, top_n=top_n)
        weights = tuple(
            FactorWeight(
                factor_name=summary.factor_name,
                weight=(summary.mean_ic or 0.0) / denominator,
                source_metric="mean_ic",
            )
            for summary in ranked
        )
        return _combine_exposures(exposures, weights, composite_name=composite_name), weights


def _select_summary_names(summaries: Sequence[FactorSummary], top_n: int | None) -> tuple[str, ...]:
    ranked = sorted(summaries, key=lambda item: ((item.mean_ic or 0.0), (item.mean_quantile_spread or 0.0)), reverse=True)
    if top_n is not None:
        ranked = ranked[:top_n]
    return tuple(summary.factor_name for summary in ranked)


def _combine_exposures(
    exposures: Sequence[FactorExposure],
    weights: Sequence[FactorWeight],
    *,
    composite_name: str,
) -> tuple[FactorExposure, ...]:
    weight_lookup = {weight.factor_name: weight.weight for weight in weights}
    grouped: dict[tuple[object, str], list[FactorExposure]] = defaultdict(list)
    for exposure in exposures:
        if exposure.factor_name in weight_lookup:
            grouped[(exposure.as_of, exposure.instrument.symbol)].append(exposure)

    combined: list[FactorExposure] = []
    for (_, _), grouped_exposures in sorted(grouped.items(), key=lambda item: item[0]):
        sample = grouped_exposures[0]
        value = sum(weight_lookup[exposure.factor_name] * exposure.value for exposure in grouped_exposures)
        combined.append(
            FactorExposure(
                factor_name=composite_name,
                as_of=sample.as_of,
                instrument=sample.instrument,
                value=value,
                feature_name="composite",
                normalization=FactorNormalization.RAW,
            )
        )
    return tuple(sorted(combined, key=lambda exposure: (exposure.as_of, exposure.instrument.symbol)))
