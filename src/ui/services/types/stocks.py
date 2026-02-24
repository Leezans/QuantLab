from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StocksUniverseRequest:
    market: str = "cn_equity"


@dataclass(frozen=True)
class StocksUniverseResult:
    source: str
    symbols: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
