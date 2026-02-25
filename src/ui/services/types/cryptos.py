from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class KlinesRangeRequestDTO:
    """Request DTO for a daily kline range get-or-create workflow."""

    symbol: str
    start: str
    end: str
    interval: str = "1m"
    market: str = "spot"
    style: str = "mirror"
    fetch_checksum: bool = True
    verify_checksum: bool = True
    compression: str = "snappy"
    raise_on_error: bool = False
    preview_rows: int = 2000
    lab_key: str = "crypto"


@dataclass
class KlinesRangeResultDTO:
    """Result DTO returned to UI for kline range queries."""

    symbol: str
    market: str
    interval: str
    source: str
    total_days: int
    ok: int
    skipped: int
    failed: int
    row_count: int
    parquet_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    preview: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass(frozen=True)
class TradesRangeRequestDTO:
    """Request DTO for a daily trades range get-or-create workflow."""

    symbol: str
    start: str
    end: str
    market: str = "spot"
    style: str = "mirror"
    fetch_checksum: bool = True
    verify_checksum: bool = True
    compression: str = "snappy"
    raise_on_error: bool = False
    preview_rows: int = 2000
    lab_key: str = "crypto"


@dataclass
class TradesRangeResultDTO:
    """Result DTO returned to UI for trades range queries."""

    symbol: str
    market: str
    source: str
    total_days: int
    ok: int
    skipped: int
    failed: int
    row_count: int
    parquet_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    preview: pd.DataFrame = field(default_factory=pd.DataFrame)


# Backward compatibility aliases.
TradesRangeRequest = TradesRangeRequestDTO
TradesRangeResult = TradesRangeResultDTO
