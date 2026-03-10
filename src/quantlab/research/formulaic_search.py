from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from statistics import fmean

from quantlab.core.models import Bar, FeatureVector
from quantlab.research.factor_evaluation import FactorEvaluator, FactorSummary, build_forward_returns
from quantlab.research.factors import FactorDefinition, FactorExposure, FactorNormalization
from quantlab.research.formulaic import FormulaNode


@dataclass(frozen=True, slots=True)
class FormulaCandidate:
    factor_name: str
    formula: str
    fitness: float
    size: int
    depth: int
    mean_ic: float | None
    mean_rank_ic: float | None
    mean_quantile_spread: float | None
    cross_sections: int
    miner: str


@dataclass(frozen=True, slots=True)
class FormulaSearchGeneration:
    generation: int
    best_fitness: float
    mean_fitness: float
    population_size: int


@dataclass(frozen=True, slots=True)
class FormulaSearchResult:
    factor_definitions: tuple[FactorDefinition, ...]
    factor_exposures: tuple[FactorExposure, ...]
    candidates: tuple[FormulaCandidate, ...]
    generations: tuple[FormulaSearchGeneration, ...]
    miner: str


class FormulaFitnessScorer:
    def __init__(
        self,
        evaluator: FactorEvaluator | None = None,
        forward_horizon: int = 1,
        complexity_penalty: float = 0.001,
        spread_weight: float = 5.0,
        rank_ic_weight: float = 0.35,
        mean_ic_weight: float = 0.55,
        hit_rate_weight: float = 0.10,
    ) -> None:
        self._evaluator = evaluator or FactorEvaluator()
        self._forward_horizon = forward_horizon
        self._complexity_penalty = complexity_penalty
        self._spread_weight = spread_weight
        self._rank_ic_weight = rank_ic_weight
        self._mean_ic_weight = mean_ic_weight
        self._hit_rate_weight = hit_rate_weight

    @property
    def evaluator(self) -> FactorEvaluator:
        return self._evaluator

    def build_forward_returns(self, bars: Sequence[Bar]) -> Mapping[tuple[str, object], float]:
        return build_forward_returns(bars, horizon=self._forward_horizon)

    def score_expression(
        self,
        expression: FormulaNode,
        features: Sequence[FeatureVector],
        forward_returns: Mapping[tuple[str, object], float],
        *,
        factor_name: str,
        miner: str,
    ) -> tuple[FormulaCandidate, tuple[FactorExposure, ...], FactorSummary]:
        exposures = evaluate_formula(expression, features, factor_name=factor_name)
        summaries = self._evaluator.evaluate(exposures, forward_returns)
        summary = summaries[0] if summaries else FactorSummary(
            factor_name=factor_name,
            observations=0,
            cross_sections=0,
            mean_ic=None,
            ic_ir=None,
            mean_rank_ic=None,
            rank_ic_ir=None,
            positive_ic_ratio=None,
            mean_quantile_spread=None,
            hit_rate=None,
            mean_top_quantile_turnover=None,
            ic_autocorrelation=None,
            rank_ic_autocorrelation=None,
        )
        fitness = self._fitness(summary, expression.size())
        candidate = FormulaCandidate(
            factor_name=factor_name,
            formula=expression.render(),
            fitness=fitness,
            size=expression.size(),
            depth=expression.depth(),
            mean_ic=summary.mean_ic,
            mean_rank_ic=summary.mean_rank_ic,
            mean_quantile_spread=summary.mean_quantile_spread,
            cross_sections=summary.cross_sections,
            miner=miner,
        )
        return candidate, exposures, summary

    def correlation(
        self,
        left: Sequence[FactorExposure],
        right: Sequence[FactorExposure],
    ) -> float | None:
        left_map = {(item.as_of, item.instrument.symbol): item.value for item in left}
        right_map = {(item.as_of, item.instrument.symbol): item.value for item in right}
        keys = sorted(set(left_map) & set(right_map))
        if len(keys) < 2:
            return None
        left_values = [left_map[key] for key in keys]
        right_values = [right_map[key] for key in keys]
        return _pearson(left_values, right_values)

    def _fitness(self, summary: FactorSummary, size: int) -> float:
        base_score = (
            self._mean_ic_weight * (summary.mean_ic or 0.0)
            + self._rank_ic_weight * (summary.mean_rank_ic or 0.0)
            + self._spread_weight * (summary.mean_quantile_spread or 0.0)
            + self._hit_rate_weight * (summary.hit_rate or 0.0)
        )
        return base_score - (self._complexity_penalty * float(size))


def evaluate_formula(
    expression: FormulaNode,
    features: Sequence[FeatureVector],
    *,
    factor_name: str,
) -> tuple[FactorExposure, ...]:
    exposures: list[FactorExposure] = []
    for vector in features:
        value = expression.evaluate({str(key): float(number) for key, number in vector.values.items()})
        exposures.append(
            FactorExposure(
                factor_name=factor_name,
                as_of=vector.as_of,
                instrument=vector.instrument,
                value=value,
                feature_name=expression.render(),
                normalization=FactorNormalization.RAW,
            )
        )
    return tuple(sorted(exposures, key=lambda item: (item.factor_name, item.as_of, item.instrument.symbol)))


def deduplicate_candidates(
    candidates: Sequence[tuple[FormulaCandidate, tuple[FactorExposure, ...], FormulaNode]],
    scorer: FormulaFitnessScorer,
    *,
    top_k: int,
    correlation_threshold: float = 0.90,
) -> tuple[tuple[FormulaCandidate, tuple[FactorExposure, ...], FormulaNode], ...]:
    selected: list[tuple[FormulaCandidate, tuple[FactorExposure, ...], FormulaNode]] = []
    for candidate, exposures, expression in sorted(candidates, key=lambda item: item[0].fitness, reverse=True):
        if any(candidate.formula == prior_candidate.formula for prior_candidate, _, _ in selected):
            continue
        if any(
            abs(scorer.correlation(exposures, prior_exposures) or 0.0) >= correlation_threshold
            for _, prior_exposures, _ in selected
        ):
            continue
        selected.append((candidate, exposures, expression))
        if len(selected) >= top_k:
            break
    return tuple(selected)


def build_search_result(
    selected: Sequence[tuple[FormulaCandidate, tuple[FactorExposure, ...], FormulaNode]],
    generations: Sequence[FormulaSearchGeneration],
    *,
    miner: str,
) -> FormulaSearchResult:
    factor_definitions = tuple(
        FactorDefinition(
            name=candidate.factor_name,
            feature_name=candidate.formula,
            sign=1.0,
            normalization=FactorNormalization.RAW,
        )
        for candidate, _, _ in selected
    )
    factor_exposures = tuple(
        exposure
        for candidate, exposures, _ in selected
        for exposure in exposures
    )
    return FormulaSearchResult(
        factor_definitions=factor_definitions,
        factor_exposures=factor_exposures,
        candidates=tuple(candidate for candidate, _, _ in selected),
        generations=tuple(generations),
        miner=miner,
    )


def summarize_generation(
    generation: int,
    candidates: Sequence[FormulaCandidate],
) -> FormulaSearchGeneration:
    fitness_values = [candidate.fitness for candidate in candidates]
    return FormulaSearchGeneration(
        generation=generation,
        best_fitness=max(fitness_values, default=float("-inf")),
        mean_fitness=fmean(fitness_values) if fitness_values else float("-inf"),
        population_size=len(candidates),
    )


def bucket_feature_vectors(features: Sequence[FeatureVector]) -> dict[object, list[FeatureVector]]:
    grouped: dict[object, list[FeatureVector]] = defaultdict(list)
    for feature in features:
        grouped[feature.as_of].append(feature)
    return grouped


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = fmean(left)
    right_mean = fmean(right)
    left_centered = [value - left_mean for value in left]
    right_centered = [value - right_mean for value in right]
    left_scale = sum(value * value for value in left_centered) ** 0.5
    right_scale = sum(value * value for value in right_centered) ** 0.5
    if left_scale <= 1e-12 or right_scale <= 1e-12:
        return None
    covariance = sum(left_value * right_value for left_value, right_value in zip(left_centered, right_centered, strict=True))
    return covariance / (left_scale * right_scale)
