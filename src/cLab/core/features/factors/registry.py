# src/cLab/core/features/factors/registry.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence


Series = Sequence[float]
SeriesDict = Mapping[str, Series]
FactorComputeFn = Callable[[SeriesDict, Mapping[str, object]], List[float]]


@dataclass(frozen=True, slots=True)
class FactorSpec:
    name: str
    compute: FactorComputeFn
    required_fields: tuple[str, ...]
    param_defaults: Dict[str, object] = field(default_factory=dict)
    description: str = ""

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("FactorSpec.name must be non-empty")
        if not callable(self.compute):
            raise ValueError("FactorSpec.compute must be callable")
        if not self.required_fields:
            raise ValueError("FactorSpec.required_fields must be non-empty")


class FactorRegistry:
    def __init__(self) -> None:
        self._specs: MutableMapping[str, FactorSpec] = {}

    def register(self, spec: FactorSpec) -> None:
        key = spec.name
        if key in self._specs:
            raise ValueError(f"Factor '{key}' is already registered")
        self._specs[key] = spec

    def get(self, name: str) -> FactorSpec:
        try:
            return self._specs[name]
        except KeyError as e:
            raise KeyError(f"Unknown factor '{name}'") from e

    def list(self) -> List[FactorSpec]:
        return sorted(self._specs.values(), key=lambda s: s.name)

    def names(self) -> List[str]:
        return [s.name for s in self.list()]

    def compute(
        self,
        name: str,
        data: SeriesDict,
        params: Optional[Mapping[str, object]] = None,
        *,
        strict_fields: bool = True,
    ) -> List[float]:
        spec = self.get(name)
        if strict_fields:
            missing = [f for f in spec.required_fields if f not in data]
            if missing:
                raise KeyError(f"Factor '{name}' missing required fields: {missing}")

        merged: Dict[str, object] = dict(spec.param_defaults)
        if params:
            merged.update(params)

        return spec.compute(data, merged)


_global_registry = FactorRegistry()


def register_factor(
    name: str,
    *,
    required_fields: Iterable[str],
    param_defaults: Optional[Mapping[str, object]] = None,
    description: str = "",
) -> Callable[[FactorComputeFn], FactorComputeFn]:
    req = tuple(required_fields)
    defaults = dict(param_defaults) if param_defaults else {}

    def _decorator(fn: FactorComputeFn) -> FactorComputeFn:
        _global_registry.register(
            FactorSpec(
                name=name,
                compute=fn,
                required_fields=req,
                param_defaults=defaults,
                description=description,
            )
        )
        return fn

    return _decorator


def get_registry() -> FactorRegistry:
    return _global_registry