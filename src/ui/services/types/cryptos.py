from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TradesRangeRequest:
    symbol: str
    start: str
    end: str
    market: str = "spot"
    style: str = "mirror"
    fetch_checksum: bool = True
    verify_checksum: bool = True
    compression: str = "snappy"
    raise_on_error: bool = False


@dataclass(frozen=True)
class TradesRangeResult:
    symbol: str
    source: str
    total_days: int
    ok: int
    skipped: int
    failed: int
    parquet_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
