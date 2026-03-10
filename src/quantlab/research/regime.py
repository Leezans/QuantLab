from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from quantlab.core.models import Signal
from quantlab.research.factor_combination import FactorWeight


@dataclass(frozen=True, slots=True)
class RegimeObservation:
    as_of: datetime
    scope: str
    values: Mapping[str, float]
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RegimeObservationFrame:
    scope: str
    observations: tuple[RegimeObservation, ...]
    feature_names: tuple[str, ...]
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RegimeModelArtifact:
    model_name: str
    scope: str
    feature_names: tuple[str, ...]
    state_count: int
    initial_probabilities: tuple[float, ...]
    transition_matrix: tuple[tuple[float, ...], ...]
    state_means: tuple[tuple[float, ...], ...]
    state_variances: tuple[tuple[float, ...], ...]
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RegimeStateEstimate:
    as_of: datetime
    scope: str
    state_id: int
    probabilities: Mapping[int, float]
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RegimeTransitionMatrix:
    scope: str
    state_ids: tuple[int, ...]
    probabilities: tuple[tuple[float, ...], ...]

    def row(self, state_id: int) -> Mapping[int, float]:
        index = self.state_ids.index(state_id)
        return {
            next_state: self.probabilities[index][next_index]
            for next_index, next_state in enumerate(self.state_ids)
        }


@dataclass(frozen=True, slots=True)
class RegimeInferenceResult:
    scope: str
    model_name: str
    states: tuple[RegimeStateEstimate, ...]
    transition_matrix: RegimeTransitionMatrix
    log_likelihood: float
    metadata: Mapping[str, str] = field(default_factory=dict)

    @property
    def latest(self) -> RegimeStateEstimate | None:
        return self.states[-1] if self.states else None


@dataclass(frozen=True, slots=True)
class RegimeDurationSummary:
    scope: str
    state_id: int
    episodes: int
    mean_duration: float
    max_duration: float
    min_duration: float


@dataclass(frozen=True, slots=True)
class RegimeProfile:
    scope: str
    state_id: int
    observations: int
    feature_means: Mapping[str, float]
    feature_stdevs: Mapping[str, float]
    mean_market_return: float | None = None
    mean_market_volatility: float | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RegimeConditionedFactorSummary:
    scope: str
    state_id: int
    factor_name: str
    observations: int
    cross_sections: int
    mean_ic: float | None
    mean_rank_ic: float | None
    mean_quantile_spread: float | None
    hit_rate: float | None
    mean_turnover: float | None
    mean_capacity_proxy: float | None


@dataclass(frozen=True, slots=True)
class RegimeConditionedDecayPoint:
    scope: str
    state_id: int
    factor_name: str
    horizon: int
    cross_sections: int
    mean_ic: float | None
    mean_rank_ic: float | None


@dataclass(frozen=True, slots=True)
class RegimeSignal:
    as_of: datetime
    scope: str
    state_id: int
    confidence: float
    transition_probabilities: Mapping[int, float]


class RegimeModel(Protocol):
    def fit(self, frame: RegimeObservationFrame) -> RegimeModelArtifact:
        ...

    def infer(self, frame: RegimeObservationFrame, artifact: RegimeModelArtifact) -> RegimeInferenceResult:
        ...


class RegimeAlphaGate:
    def __init__(self, allowed_states: Mapping[str, Sequence[int]]) -> None:
        self._allowed_states = {
            str(signal_name): frozenset(int(state_id) for state_id in state_ids)
            for signal_name, state_ids in allowed_states.items()
        }

    def apply(self, signals: Sequence[Signal], regime_signal: RegimeSignal) -> tuple[Signal, ...]:
        allowed = self._allowed_states
        return tuple(
            signal
            for signal in signals
            if signal.name not in allowed or regime_signal.state_id in allowed[signal.name]
        )


class RegimeFactorWeightOverlay:
    def __init__(self, state_multipliers: Mapping[int, Mapping[str, float]]) -> None:
        self._state_multipliers = {
            int(state_id): {str(factor_name): float(multiplier) for factor_name, multiplier in multipliers.items()}
            for state_id, multipliers in state_multipliers.items()
        }

    def apply(self, weights: Sequence[FactorWeight], regime_signal: RegimeSignal) -> tuple[FactorWeight, ...]:
        multipliers = self._state_multipliers.get(regime_signal.state_id, {})
        return tuple(
            FactorWeight(
                factor_name=weight.factor_name,
                weight=weight.weight * multipliers.get(weight.factor_name, 1.0),
                source_metric=f"{weight.source_metric}|regime={regime_signal.state_id}",
            )
            for weight in weights
        )


class RegimeRiskOverlay:
    def __init__(self, gross_exposure_by_state: Mapping[int, float]) -> None:
        self._gross_exposure_by_state = {int(state_id): float(limit) for state_id, limit in gross_exposure_by_state.items()}

    def gross_target(self, regime_signal: RegimeSignal, default: float) -> float:
        return self._gross_exposure_by_state.get(regime_signal.state_id, default)


class RegimeExecutionSwitch:
    def __init__(self, execution_mode_by_state: Mapping[int, str]) -> None:
        self._execution_mode_by_state = {int(state_id): str(mode) for state_id, mode in execution_mode_by_state.items()}

    def resolve(self, regime_signal: RegimeSignal, default: str = "immediate") -> str:
        return self._execution_mode_by_state.get(regime_signal.state_id, default)
