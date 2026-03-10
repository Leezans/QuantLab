from __future__ import annotations

from collections.abc import Callable, Sequence

from quantlab.core.models import Bar

BarTransform = Callable[[Sequence[Bar]], Sequence[Bar]]


class BarTransformPipeline:
    def __init__(self, transforms: Sequence[BarTransform] | None = None) -> None:
        self._transforms = tuple(transforms or ())

    def run(self, bars: Sequence[Bar]) -> tuple[Bar, ...]:
        transformed: Sequence[Bar] = tuple(bars)
        for transform in self._transforms:
            transformed = tuple(transform(transformed))
        return tuple(transformed)


def filter_zero_volume(bars: Sequence[Bar]) -> tuple[Bar, ...]:
    return tuple(bar for bar in bars if bar.volume > 0)

