from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class KlinesRequestDTO:
    """Request DTO for klines get-or-create workflow."""

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
class KlinesResultDTO:
    """Result DTO returned to UI for klines queries."""

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
class TradesRequestDTO:
    """Request DTO for trades get-or-create workflow."""

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
class TradesResultDTO:
    """Result DTO returned to UI for trades queries."""

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


@dataclass(frozen=True)
class VolumeProfileDTO:
    """Computed volume profile over price bins."""

    bin_centers: list[float] = field(default_factory=list)
    volumes: list[float] = field(default_factory=list)
    volume_type: str = "base"
    normalized: bool = False


# Backward compatibility aliases for previous naming.
KlinesRangeRequestDTO = KlinesRequestDTO
KlinesRangeResultDTO = KlinesResultDTO
TradesRangeRequestDTO = TradesRequestDTO
TradesRangeResultDTO = TradesResultDTO
TradesRangeRequest = TradesRequestDTO
TradesRangeResult = TradesResultDTO
