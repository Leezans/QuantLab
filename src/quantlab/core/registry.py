from __future__ import annotations

from typing import Generic, TypeVar

T = TypeVar("T")


class ComponentRegistry(Generic[T]):
    """Minimal registry for pluggable platform components."""

    def __init__(self) -> None:
        self._components: dict[str, T] = {}

    def register(self, name: str, component: T) -> T:
        if name in self._components:
            raise ValueError(f"component already registered: {name}")
        self._components[name] = component
        return component

    def get(self, name: str) -> T:
        try:
            return self._components[name]
        except KeyError as exc:
            raise KeyError(f"unknown component: {name}") from exc

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._components))

