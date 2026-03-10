from __future__ import annotations

from dataclasses import dataclass
from math import exp, log, pi
from random import Random
from statistics import fmean, pstdev

from quantlab.research.regime import (
    RegimeInferenceResult,
    RegimeModelArtifact,
    RegimeObservationFrame,
    RegimeStateEstimate,
    RegimeTransitionMatrix,
)


@dataclass(frozen=True, slots=True)
class GaussianHMMConfig:
    n_states: int = 3
    max_iterations: int = 20
    tolerance: float = 1e-4
    covariance_floor: float = 1e-6
    smoothing: float = 1e-3
    random_seed: int = 17


class GaussianHMMRegimeModel:
    def __init__(self, config: GaussianHMMConfig | None = None) -> None:
        self._config = config or GaussianHMMConfig()

    @property
    def config(self) -> GaussianHMMConfig:
        return self._config

    def fit(self, frame: RegimeObservationFrame) -> RegimeModelArtifact:
        observations = _matrix_from_frame(frame)
        if not observations:
            raise ValueError("cannot fit a regime model on an empty observation frame")
        state_count = min(self._config.n_states, len(observations))
        assignments = _initialize_assignments(observations, state_count, self._config.random_seed)
        previous_log_likelihood: float | None = None
        artifact = self._estimate_parameters(frame, observations, assignments)
        for _ in range(self._config.max_iterations):
            decoded, _ = self._viterbi(observations, artifact)
            artifact = self._estimate_parameters(frame, observations, decoded)
            inference = self.infer(frame, artifact)
            if decoded == assignments:
                break
            if previous_log_likelihood is not None and abs(inference.log_likelihood - previous_log_likelihood) <= self._config.tolerance:
                break
            assignments = decoded
            previous_log_likelihood = inference.log_likelihood
        final_inference = self.infer(frame, artifact)
        return RegimeModelArtifact(
            model_name=artifact.model_name,
            scope=artifact.scope,
            feature_names=artifact.feature_names,
            state_count=artifact.state_count,
            initial_probabilities=artifact.initial_probabilities,
            transition_matrix=artifact.transition_matrix,
            state_means=artifact.state_means,
            state_variances=artifact.state_variances,
            metadata={
                **artifact.metadata,
                "trainer": "viterbi_em",
                "log_likelihood": f"{final_inference.log_likelihood:.8f}",
            },
        )

    def infer(self, frame: RegimeObservationFrame, artifact: RegimeModelArtifact) -> RegimeInferenceResult:
        observations = _matrix_from_frame(frame, feature_names=artifact.feature_names)
        if not observations:
            return RegimeInferenceResult(
                scope=frame.scope,
                model_name=artifact.model_name,
                states=(),
                transition_matrix=RegimeTransitionMatrix(
                    scope=frame.scope,
                    state_ids=tuple(range(artifact.state_count)),
                    probabilities=artifact.transition_matrix,
                ),
                log_likelihood=0.0,
            )
        posteriors, log_likelihood = _forward_backward(observations, artifact)
        decoded, _ = self._viterbi(observations, artifact)
        states = tuple(
            RegimeStateEstimate(
                as_of=observation.as_of,
                scope=frame.scope,
                state_id=decoded[index],
                probabilities={state_id: posteriors[index][state_id] for state_id in range(artifact.state_count)},
            )
            for index, observation in enumerate(frame.observations)
        )
        return RegimeInferenceResult(
            scope=frame.scope,
            model_name=artifact.model_name,
            states=states,
            transition_matrix=RegimeTransitionMatrix(
                scope=frame.scope,
                state_ids=tuple(range(artifact.state_count)),
                probabilities=artifact.transition_matrix,
            ),
            log_likelihood=log_likelihood,
            metadata={"feature_count": str(len(artifact.feature_names))},
        )

    def _estimate_parameters(
        self,
        frame: RegimeObservationFrame,
        observations: list[list[float]],
        assignments: list[int],
    ) -> RegimeModelArtifact:
        state_count = max(assignments) + 1
        feature_count = len(frame.feature_names)
        overall_means = [fmean(row[column] for row in observations) for column in range(feature_count)]
        overall_variances = [
            max(self._config.covariance_floor, (pstdev([row[column] for row in observations]) if len(observations) > 1 else 0.0) ** 2)
            for column in range(feature_count)
        ]
        grouped: list[list[list[float]]] = [[] for _ in range(state_count)]
        for assignment, row in zip(assignments, observations, strict=True):
            grouped[assignment].append(row)

        means: list[list[float]] = []
        variances: list[list[float]] = []
        for state_rows in grouped:
            if not state_rows:
                means.append(list(overall_means))
                variances.append(list(overall_variances))
                continue
            means.append([fmean(row[column] for row in state_rows) for column in range(feature_count)])
            variances.append(
                [
                    max(
                        self._config.covariance_floor,
                        (pstdev([row[column] for row in state_rows]) if len(state_rows) > 1 else 0.0) ** 2,
                    )
                    for column in range(feature_count)
                ]
            )

        initial = [self._config.smoothing for _ in range(state_count)]
        initial[assignments[0]] += 1.0
        initial = _normalize(initial)

        transition_counts = [
            [self._config.smoothing for _ in range(state_count)]
            for _ in range(state_count)
        ]
        for left, right in zip(assignments[:-1], assignments[1:], strict=True):
            transition_counts[left][right] += 1.0
        transition_matrix = tuple(tuple(_normalize(row)) for row in transition_counts)

        ordered = sorted(range(state_count), key=lambda state_id: means[state_id][0] if means[state_id] else 0.0)
        remap = {old_state: new_state for new_state, old_state in enumerate(ordered)}
        reordered_initial = tuple(initial[state_id] for state_id in ordered)
        reordered_means = tuple(tuple(means[state_id]) for state_id in ordered)
        reordered_variances = tuple(tuple(variances[state_id]) for state_id in ordered)
        reordered_transition = tuple(
            tuple(transition_matrix[old_left][old_right] for old_right in ordered)
            for old_left in ordered
        )
        return RegimeModelArtifact(
            model_name="gaussian_hmm",
            scope=frame.scope,
            feature_names=frame.feature_names,
            state_count=state_count,
            initial_probabilities=reordered_initial,
            transition_matrix=reordered_transition,
            state_means=reordered_means,
            state_variances=reordered_variances,
            metadata={
                "state_ordering": ",".join(f"{old_state}->{remap[old_state]}" for old_state in sorted(remap)),
            },
        )

    def _viterbi(self, observations: list[list[float]], artifact: RegimeModelArtifact) -> tuple[list[int], float]:
        state_count = artifact.state_count
        emissions = _emission_log_probabilities(observations, artifact)
        score = [[float("-inf")] * state_count for _ in observations]
        backpointer = [[0] * state_count for _ in observations]
        for state_id in range(state_count):
            score[0][state_id] = log(artifact.initial_probabilities[state_id]) + emissions[0][state_id]
        for index in range(1, len(observations)):
            for state_id in range(state_count):
                candidates = [
                    score[index - 1][previous_state] + log(artifact.transition_matrix[previous_state][state_id])
                    for previous_state in range(state_count)
                ]
                best_previous = max(range(state_count), key=lambda previous_state: candidates[previous_state])
                score[index][state_id] = candidates[best_previous] + emissions[index][state_id]
                backpointer[index][state_id] = best_previous
        last_state = max(range(state_count), key=lambda state_id: score[-1][state_id])
        path = [last_state]
        for index in range(len(observations) - 1, 0, -1):
            path.append(backpointer[index][path[-1]])
        path.reverse()
        return path, max(score[-1])


def _matrix_from_frame(frame: RegimeObservationFrame, feature_names: tuple[str, ...] | None = None) -> list[list[float]]:
    resolved = feature_names or frame.feature_names
    return [
        [float(observation.values[name]) for name in resolved]
        for observation in frame.observations
    ]


def _initialize_assignments(observations: list[list[float]], state_count: int, random_seed: int) -> list[int]:
    if state_count <= 1:
        return [0 for _ in observations]
    feature_index = 0
    order = sorted(range(len(observations)), key=lambda index: observations[index][feature_index])
    assignments = [0 for _ in observations]
    bucket = max(1, len(observations) // state_count)
    for rank, observation_index in enumerate(order):
        assignments[observation_index] = min(state_count - 1, rank // bucket)
    rng = Random(random_seed)
    if len(set(assignments)) < state_count:
        for index in range(len(assignments)):
            assignments[index] = rng.randrange(state_count)
    return assignments


def _forward_backward(observations: list[list[float]], artifact: RegimeModelArtifact) -> tuple[list[list[float]], float]:
    state_count = artifact.state_count
    emissions = _emission_log_probabilities(observations, artifact)
    forward = [[float("-inf")] * state_count for _ in observations]
    backward = [[0.0] * state_count for _ in observations]

    for state_id in range(state_count):
        forward[0][state_id] = log(artifact.initial_probabilities[state_id]) + emissions[0][state_id]
    for index in range(1, len(observations)):
        for state_id in range(state_count):
            forward[index][state_id] = emissions[index][state_id] + _logsumexp(
                forward[index - 1][previous_state] + log(artifact.transition_matrix[previous_state][state_id])
                for previous_state in range(state_count)
            )

    for index in range(len(observations) - 2, -1, -1):
        for state_id in range(state_count):
            backward[index][state_id] = _logsumexp(
                log(artifact.transition_matrix[state_id][next_state]) + emissions[index + 1][next_state] + backward[index + 1][next_state]
                for next_state in range(state_count)
            )

    log_likelihood = _logsumexp(forward[-1])
    posteriors: list[list[float]] = []
    for index in range(len(observations)):
        row = [exp(forward[index][state_id] + backward[index][state_id] - log_likelihood) for state_id in range(state_count)]
        posteriors.append(_normalize(row))
    return posteriors, log_likelihood


def _emission_log_probabilities(observations: list[list[float]], artifact: RegimeModelArtifact) -> list[list[float]]:
    output: list[list[float]] = []
    for row in observations:
        output.append(
            [
                _gaussian_logpdf(row, artifact.state_means[state_id], artifact.state_variances[state_id])
                for state_id in range(artifact.state_count)
            ]
        )
    return output


def _gaussian_logpdf(observation: list[float], mean: tuple[float, ...], variance: tuple[float, ...]) -> float:
    total = 0.0
    for value, location, scale in zip(observation, mean, variance, strict=True):
        total += -0.5 * (log(2.0 * pi * scale) + ((value - location) ** 2) / scale)
    return total


def _normalize(values: list[float]) -> list[float]:
    total = sum(values)
    if total <= 1e-12:
        return [1.0 / len(values) for _ in values]
    return [value / total for value in values]


def _logsumexp(values) -> float:
    values = list(values)
    baseline = max(values)
    if baseline == float("-inf"):
        return baseline
    return baseline + log(sum(exp(value - baseline) for value in values))
