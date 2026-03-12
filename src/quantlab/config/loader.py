from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Mapping

from quantlab.config.models import (
    ExecutionSettings,
    ProjectSettings,
    QuantLabSettings,
    ResearchSettings,
    RuntimeSettings,
    StorageSettings,
)


def _section(data: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = data.get(name, {})
    if not isinstance(value, dict):
        raise TypeError(f"config section '{name}' must be a table")
    return value


def _path(value: Any, default: Path) -> Path:
    if value is None:
        return default.expanduser()
    return Path(value).expanduser()


def load_settings(path: str | Path) -> QuantLabSettings:
    config_path = Path(path)
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)

    project = _section(data, "project")
    storage = _section(data, "storage")
    research = _section(data, "research")
    runtime = _section(data, "runtime")
    execution = _section(data, "execution")

    return QuantLabSettings(
        project=ProjectSettings(
            name=project.get("name", "QuantLab"),
            environment=project.get("environment", "research"),
        ),
        storage=StorageSettings(
            raw_data_dir=_path(storage.get("raw_data_dir"), StorageSettings().raw_data_dir),
            curated_data_dir=_path(storage.get("curated_data_dir"), StorageSettings().curated_data_dir),
            feature_store_dir=_path(storage.get("feature_store_dir"), StorageSettings().feature_store_dir),
            intraday_cache_dir=_path(storage.get("intraday_cache_dir"), StorageSettings().intraday_cache_dir),
            warehouse_dir=_path(storage.get("warehouse_dir"), StorageSettings().warehouse_dir),
            catalog_path=_path(storage.get("catalog_path"), StorageSettings().catalog_path),
            duckdb_path=_path(storage.get("duckdb_path"), StorageSettings().duckdb_path),
            artifact_dir=_path(storage.get("artifact_dir"), StorageSettings().artifact_dir),
        ),
        research=ResearchSettings(
            default_universe=tuple(research.get("default_universe", ["BTCUSDT"])),
            primary_frequency=research.get("primary_frequency", "1d"),
            base_currency=research.get("base_currency", "USD"),
            signal_threshold=float(research.get("signal_threshold", 10000.0)),
        ),
        runtime=RuntimeSettings(
            timezone=runtime.get("timezone", "UTC"),
            max_workers=int(runtime.get("max_workers", 4)),
            process_workers=int(runtime.get("process_workers", 2)),
            queue_poll_timeout=float(runtime.get("queue_poll_timeout", 0.5)),
        ),
        execution=ExecutionSettings(
            paper_trading=bool(execution.get("paper_trading", True)),
            default_slippage_bps=float(execution.get("default_slippage_bps", 5.0)),
        ),
    )
