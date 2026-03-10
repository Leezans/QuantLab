from __future__ import annotations

from collections import defaultdict
from typing import Sequence

from quantlab.core.models import PortfolioSnapshot, Signal, TargetPosition


class SignalWeightedStrategy:
    def __init__(self, max_weight_per_instrument: float = 0.10) -> None:
        self._max_weight_per_instrument = max_weight_per_instrument

    def generate_targets(
        self,
        signals: Sequence[Signal],
        portfolio: PortfolioSnapshot,
    ) -> tuple[TargetPosition, ...]:
        del portfolio
        score_by_symbol: dict[str, float] = defaultdict(float)
        latest_by_symbol: dict[str, Signal] = {}
        reasons: dict[str, list[str]] = defaultdict(list)

        for signal in signals:
            score_by_symbol[signal.instrument.symbol] += signal.value
            latest_by_symbol[signal.instrument.symbol] = signal
            reasons[signal.instrument.symbol].append(signal.name)

        gross_score = sum(abs(score) for score in score_by_symbol.values()) or 1.0
        targets: list[TargetPosition] = []
        for symbol, score in score_by_symbol.items():
            signal = latest_by_symbol[symbol]
            unclipped_weight = score / gross_score
            target_weight = max(-self._max_weight_per_instrument, min(self._max_weight_per_instrument, unclipped_weight))
            targets.append(
                TargetPosition(
                    as_of=signal.as_of,
                    instrument=signal.instrument,
                    target_weight=target_weight,
                    reason=",".join(sorted(reasons[symbol])),
                    signal_name=signal.name,
                )
            )
        return tuple(targets)

