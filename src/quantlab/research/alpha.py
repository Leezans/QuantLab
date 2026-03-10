from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from quantlab.core.enums import SignalDirection
from quantlab.core.models import FeatureVector, Signal

AlphaScorer = Callable[[FeatureVector], float]


@dataclass(frozen=True, slots=True)
class AlphaDefinition:
    name: str
    score: AlphaScorer
    threshold: float = 0.0


class AlphaFactory:
    def __init__(self, definitions: Sequence[AlphaDefinition]) -> None:
        self._definitions = tuple(definitions)

    def generate(self, features: Sequence[FeatureVector]) -> tuple[Signal, ...]:
        signals: list[Signal] = []
        for vector in features:
            for definition in self._definitions:
                score = float(definition.score(vector))
                if score > definition.threshold:
                    direction = SignalDirection.LONG
                elif score < -definition.threshold:
                    direction = SignalDirection.SHORT
                else:
                    direction = SignalDirection.FLAT

                if direction is SignalDirection.FLAT:
                    continue

                signals.append(
                    Signal(
                        as_of=vector.as_of,
                        instrument=vector.instrument,
                        name=definition.name,
                        value=score,
                        direction=direction,
                        confidence=min(1.0, abs(score)),
                        metadata={"feature_names": ",".join(sorted(vector.values))},
                    )
                )
        return tuple(signals)

