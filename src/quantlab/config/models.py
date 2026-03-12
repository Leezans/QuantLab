from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os


def _default_crypto_data_root() -> Path:
    if os.name == "nt":
        # return Path("G:/database/crypto")
        return Path.home() / "Documents" / "QuantDatabase" / "crypto"
    elif os.name == "posix":
        return Path.home() / "Documents" / "QuantDatabase" / "crypto"
    raise RuntimeError(f"Unsupported OS: {os.name}")


def _default_raw_data_dir() -> Path:
    return _default_crypto_data_root() / "raw"


def _default_curated_data_dir() -> Path:
    return _default_crypto_data_root() / "curated"


def _default_feature_store_dir() -> Path:
    return _default_crypto_data_root() / "features"


def _default_intraday_cache_dir() -> Path:
    return _default_crypto_data_root() / "intraday_cache"


def _default_warehouse_dir() -> Path:
    return _default_crypto_data_root() / "warehouse"


def _default_catalog_path() -> Path:
    return _default_crypto_data_root() / "catalog" / "catalog.json"


def _default_duckdb_path() -> Path:
    return _default_crypto_data_root() / "artifacts" / "quantlab.duckdb"


def _default_artifact_dir() -> Path:
    return _default_crypto_data_root() / "artifacts"


@dataclass(frozen=True, slots=True)
class ProjectSettings:
    name: str = "QuantLab"
    environment: str = "research"


@dataclass(frozen=True, slots=True)
class StorageSettings:
    raw_data_dir: Path = field(default_factory=_default_raw_data_dir)
    curated_data_dir: Path = field(default_factory=_default_curated_data_dir)
    feature_store_dir: Path = field(default_factory=_default_feature_store_dir)
    intraday_cache_dir: Path = field(default_factory=_default_intraday_cache_dir)
    warehouse_dir: Path = field(default_factory=_default_warehouse_dir)
    catalog_path: Path = field(default_factory=_default_catalog_path)
    duckdb_path: Path = field(default_factory=_default_duckdb_path)
    artifact_dir: Path = field(default_factory=_default_artifact_dir)


@dataclass(frozen=True, slots=True)
class ResearchSettings:
    default_universe: tuple[str, ...] = ("BTCUSDT",)
    primary_frequency: str = "1d"
    base_currency: str = "USD"
    signal_threshold: float = 10000.0


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    timezone: str = "UTC"
    max_workers: int = 4


@dataclass(frozen=True, slots=True)
class ExecutionSettings:
    paper_trading: bool = True
    default_slippage_bps: float = 5.0


@dataclass(frozen=True, slots=True)
class QuantLabSettings:
    project: ProjectSettings = field(default_factory=ProjectSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    research: ResearchSettings = field(default_factory=ResearchSettings)
    runtime: RuntimeSettings = field(default_factory=RuntimeSettings)
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)
