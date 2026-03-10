from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from random import Random

from quantlab.core.models import Bar, FeatureVector
from quantlab.research.formulaic import (
    FeatureSignature,
    FormulaNode,
    infer_feature_signatures,
    iter_paths,
    random_formula_tree,
    replace_subtree,
    subtree_at,
)
from quantlab.research.formulaic_search import (
    FormulaCandidate,
    FormulaFitnessScorer,
    FormulaSearchResult,
    build_search_result,
    deduplicate_candidates,
    summarize_generation,
)


@dataclass(frozen=True, slots=True)
class GeneticProgrammingConfig:
    population_size: int = 80
    generations: int = 12
    tournament_size: int = 5
    max_depth: int = 4
    elite_size: int = 8
    crossover_rate: float = 0.55
    subtree_mutation_rate: float = 0.25
    point_mutation_rate: float = 0.10
    top_k: int = 8
    random_seed: int = 7


class GeneticProgrammingFactorMiner:
    def __init__(
        self,
        config: GeneticProgrammingConfig | None = None,
        scorer: FormulaFitnessScorer | None = None,
    ) -> None:
        self._config = config or GeneticProgrammingConfig()
        self._scorer = scorer or FormulaFitnessScorer()

    @property
    def config(self) -> GeneticProgrammingConfig:
        return self._config

    def mine(self, features: Sequence[FeatureVector], bars: Sequence[Bar]) -> FormulaSearchResult:
        if not features:
            return build_search_result((), (), miner="gp")
        rng = Random(self._config.random_seed)
        signatures = infer_feature_signatures({name for vector in features for name in vector.values})
        forward_returns = self._scorer.build_forward_returns(bars)
        population = [
            random_formula_tree(
                rng,
                signatures,
                max_depth=self._config.max_depth,
                grow=index >= self._config.population_size // 2,
            )
            for index in range(self._config.population_size)
        ]
        cache: dict[str, tuple[FormulaCandidate, tuple, FormulaNode]] = {}
        generation_summaries = []
        hall_of_fame: list[tuple[FormulaCandidate, tuple, FormulaNode]] = []

        for generation in range(self._config.generations):
            evaluated = [
                self._evaluate(expression, features, forward_returns, cache)
                for expression in population
            ]
            candidates = [candidate for candidate, _, _ in evaluated]
            generation_summaries.append(summarize_generation(generation, candidates))
            hall_of_fame.extend(evaluated)
            elites = [
                expression
                for _, _, expression in sorted(evaluated, key=lambda item: item[0].fitness, reverse=True)[: self._config.elite_size]
            ]
            next_population = list(elites)
            while len(next_population) < self._config.population_size:
                draw = rng.random()
                if draw < self._config.crossover_rate:
                    left = self._tournament(evaluated, rng)
                    right = self._tournament(evaluated, rng)
                    next_population.append(self._crossover(left, right, rng))
                    continue
                if draw < self._config.crossover_rate + self._config.subtree_mutation_rate:
                    parent = self._tournament(evaluated, rng)
                    next_population.append(self._subtree_mutation(parent, signatures, rng))
                    continue
                if draw < self._config.crossover_rate + self._config.subtree_mutation_rate + self._config.point_mutation_rate:
                    parent = self._tournament(evaluated, rng)
                    next_population.append(self._point_mutation(parent, signatures, rng))
                    continue
                next_population.append(self._tournament(evaluated, rng))
            population = next_population[: self._config.population_size]

        selected = deduplicate_candidates(
            hall_of_fame,
            self._scorer,
            top_k=self._config.top_k,
        )
        return build_search_result(selected, generation_summaries, miner="gp")

    def _evaluate(
        self,
        expression: FormulaNode,
        features: Sequence[FeatureVector],
        forward_returns,
        cache: dict[str, tuple[FormulaCandidate, tuple, FormulaNode]],
    ) -> tuple[FormulaCandidate, tuple, FormulaNode]:
        formula = expression.render()
        if formula in cache:
            return cache[formula]
        factor_name = f"gp::{abs(hash(formula)) % 1_000_000:06d}"
        candidate, exposures, _ = self._scorer.score_expression(
            expression,
            features,
            forward_returns,
            factor_name=factor_name,
            miner="gp",
        )
        result = (candidate, exposures, expression)
        cache[formula] = result
        return result

    def _tournament(
        self,
        evaluated: Sequence[tuple[FormulaCandidate, tuple, FormulaNode]],
        rng: Random,
    ) -> FormulaNode:
        sampled = [rng.choice(tuple(evaluated)) for _ in range(min(self._config.tournament_size, len(evaluated)))]
        winner = max(sampled, key=lambda item: item[0].fitness)
        return winner[2]

    def _crossover(self, left: FormulaNode, right: FormulaNode, rng: Random) -> FormulaNode:
        left_paths = iter_paths(left)
        right_paths = iter_paths(right)
        left_path = rng.choice(left_paths)
        left_subtree = subtree_at(left, left_path)
        compatible_paths = [path for path in right_paths if subtree_at(right, path).dimension == left_subtree.dimension]
        if not compatible_paths:
            return left
        replacement = subtree_at(right, rng.choice(compatible_paths))
        return replace_subtree(left, left_path, replacement)

    def _subtree_mutation(
        self,
        parent: FormulaNode,
        signatures: Sequence[FeatureSignature],
        rng: Random,
    ) -> FormulaNode:
        path = rng.choice(iter_paths(parent))
        replacement = random_formula_tree(
            rng,
            signatures,
            max_depth=max(2, self._config.max_depth - len(path)),
            grow=True,
        )
        return replace_subtree(parent, path, replacement)

    def _point_mutation(
        self,
        parent: FormulaNode,
        signatures: Sequence[FeatureSignature],
        rng: Random,
    ) -> FormulaNode:
        path = rng.choice(iter_paths(parent))
        target = subtree_at(parent, path)
        if target.kind == "feature" and signatures:
            replacement = random_formula_tree(rng, signatures, max_depth=1)
        else:
            replacement = random_formula_tree(rng, signatures, max_depth=max(1, target.depth()), grow=True)
        return replace_subtree(parent, path, replacement)
