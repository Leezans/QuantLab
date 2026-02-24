from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FuturesUniverseRequest:
    market: str = "cn_futures"


@dataclass(frozen=True)
class FuturesUniverseResult:
    source: str
    symbols: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
