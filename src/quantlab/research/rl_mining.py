from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from math import exp
from random import Random

from quantlab.core.models import Bar, FeatureVector
from quantlab.research.formulaic import (
    DEFAULT_BINARY_OPERATORS,
    DEFAULT_UNARY_OPERATORS,
    FeatureSignature,
    FormulaNode,
    binary_node,
    constant_node,
    feature_node,
    infer_feature_signatures,
    unary_node,
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
class PolicyGradientConfig:
    episodes: int = 12
    samples_per_episode: int = 24
    max_depth: int = 4
    learning_rate: float = 0.08
    entropy_floor: float = 0.02
    top_k: int = 8
    random_seed: int = 17


class PolicyGradientFactorMiner:
    def __init__(
        self,
        config: PolicyGradientConfig | None = None,
        scorer: FormulaFitnessScorer | None = None,
    ) -> None:
        self._config = config or PolicyGradientConfig()
        self._scorer = scorer or FormulaFitnessScorer()

    @property
    def config(self) -> PolicyGradientConfig:
        return self._config

    def mine(self, features: Sequence[FeatureVector], bars: Sequence[Bar]) -> FormulaSearchResult:
        if not features:
            return build_search_result((), (), miner="policy_gradient")
        rng = Random(self._config.random_seed)
        signatures = infer_feature_signatures({name for vector in features for name in vector.values})
        forward_returns = self._scorer.build_forward_returns(bars)
        policy = _PolicyState(feature_count=len(signatures), max_depth=self._config.max_depth)
        cache: dict[str, tuple[FormulaCandidate, tuple, FormulaNode]] = {}
        hall_of_fame: list[tuple[FormulaCandidate, tuple, FormulaNode]] = []
        generation_summaries = []

        for episode in range(self._config.episodes):
            sampled: list[tuple[FormulaNode, list[tuple[list[float], int]]]] = [
                self._sample_expression(policy, signatures, rng)
                for _ in range(self._config.samples_per_episode)
            ]
            evaluated = [
                self._evaluate(expression, features, forward_returns, cache)
                for expression, _ in sampled
            ]
            hall_of_fame.extend(evaluated)
            candidates = [candidate for candidate, _, _ in evaluated]
            generation_summaries.append(summarize_generation(episode, candidates))
            baseline = sum(candidate.fitness for candidate in candidates) / len(candidates)
            for (expression, log), (candidate, _, _) in zip(sampled, evaluated, strict=True):
                del expression
                advantage = candidate.fitness - baseline
                for logits, action_index in log:
                    _apply_reinforce_update(
                        logits,
                        action_index,
                        advantage=advantage,
                        learning_rate=self._config.learning_rate,
                        entropy_floor=self._config.entropy_floor,
                    )

        selected = deduplicate_candidates(
            hall_of_fame,
            self._scorer,
            top_k=self._config.top_k,
        )
        return build_search_result(selected, generation_summaries, miner="policy_gradient")

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
        factor_name = f"rl::{abs(hash(formula)) % 1_000_000:06d}"
        candidate, exposures, _ = self._scorer.score_expression(
            expression,
            features,
            forward_returns,
            factor_name=factor_name,
            miner="policy_gradient",
        )
        result = (candidate, exposures, expression)
        cache[formula] = result
        return result

    def _sample_expression(
        self,
        policy: "_PolicyState",
        signatures: Sequence[FeatureSignature],
        rng: Random,
    ) -> tuple[FormulaNode, list[tuple[list[float], int]]]:
        log: list[tuple[list[float], int]] = []

        def sample(depth: int) -> FormulaNode:
            if depth >= self._config.max_depth - 1:
                return self._sample_terminal(policy, signatures, depth, log, rng)
            action_index = _sample_logits(policy.expand_logits[depth], rng)
            log.append((policy.expand_logits[depth], action_index))
            if action_index == 0:
                return self._sample_terminal(policy, signatures, depth, log, rng)
            if action_index == 1:
                operator_index = _sample_logits(policy.unary_logits[depth], rng)
                log.append((policy.unary_logits[depth], operator_index))
                return unary_node(DEFAULT_UNARY_OPERATORS[operator_index], sample(depth + 1))
            operator_index = _sample_logits(policy.binary_logits[depth], rng)
            log.append((policy.binary_logits[depth], operator_index))
            return binary_node(
                DEFAULT_BINARY_OPERATORS[operator_index],
                sample(depth + 1),
                sample(depth + 1),
            )

        return sample(0), log

    def _sample_terminal(
        self,
        policy: "_PolicyState",
        signatures: Sequence[FeatureSignature],
        depth: int,
        log: list[tuple[list[float], int]],
        rng: Random,
    ) -> FormulaNode:
        action_index = _sample_logits(policy.terminal_kind_logits[depth], rng)
        log.append((policy.terminal_kind_logits[depth], action_index))
        if action_index == 0 and signatures:
            feature_index = _sample_logits(policy.feature_logits[depth], rng)
            log.append((policy.feature_logits[depth], feature_index))
            return feature_node(signatures[feature_index % len(signatures)])
        constant_value = rng.uniform(-2.0, 2.0)
        return constant_node(constant_value)


class _PolicyState:
    def __init__(self, feature_count: int, max_depth: int) -> None:
        self.expand_logits = [[0.0, -0.2, 0.2] for _ in range(max_depth)]
        self.unary_logits = [[0.0 for _ in DEFAULT_UNARY_OPERATORS] for _ in range(max_depth)]
        self.binary_logits = [[0.0 for _ in DEFAULT_BINARY_OPERATORS] for _ in range(max_depth)]
        self.terminal_kind_logits = [[0.4, -0.1] for _ in range(max_depth)]
        self.feature_logits = [[0.0 for _ in range(max(feature_count, 1))] for _ in range(max_depth)]


def _apply_reinforce_update(
    logits: list[float],
    action_index: int,
    *,
    advantage: float,
    learning_rate: float,
    entropy_floor: float,
) -> None:
    probabilities = _softmax(logits)
    for index in range(len(logits)):
        gradient = (1.0 if index == action_index else 0.0) - probabilities[index]
        logits[index] += learning_rate * advantage * gradient
        logits[index] *= (1.0 - entropy_floor)


def _sample_logits(logits: Sequence[float], rng: Random) -> int:
    probabilities = _softmax(logits)
    draw = rng.random()
    cumulative = 0.0
    for index, probability in enumerate(probabilities):
        cumulative += probability
        if draw <= cumulative:
            return index
    return len(probabilities) - 1


def _softmax(logits: Sequence[float]) -> list[float]:
    shifted = [value - max(logits) for value in logits]
    exponentials = [exp(value) for value in shifted]
    total = sum(exponentials)
    if total <= 1e-12:
        return [1.0 / len(logits) for _ in logits]
    return [value / total for value in exponentials]
