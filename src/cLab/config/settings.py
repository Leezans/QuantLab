from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StorageSettings:
    root_dir: Path
    bars_dir: Path
    features_dir: Path
    runs_dir: Path
    artifacts_dir: Path


@dataclass(frozen=True)
class FeatureFlags:
    enable_redis_cache: bool
    enable_duckdb_store: bool


@dataclass(frozen=True)
class ApiSettings:
    host: str
    port: int
    reload: bool


@dataclass(frozen=True)
class Settings:
    env: str
    storage: StorageSettings
    flags: FeatureFlags
    api: ApiSettings


def load_settings() -> Settings:
    env = os.getenv("CLAB_ENV", "dev").strip() or "dev"

    root_dir = Path(os.getenv("CLAB_STORAGE_ROOT", "./.clab_storage")).resolve()
    bars_dir = Path(os.getenv("CLAB_BARS_DIR", str(root_dir / "bars"))).resolve()
    features_dir = Path(os.getenv("CLAB_FEATURES_DIR", str(root_dir / "features"))).resolve()
    runs_dir = Path(os.getenv("CLAB_RUNS_DIR", str(root_dir / "runs"))).resolve()
    artifacts_dir = Path(os.getenv("CLAB_ARTIFACTS_DIR", str(root_dir / "artifacts"))).resolve()

    host = os.getenv("CLAB_API_HOST", "127.0.0.1")
    port = int(os.getenv("CLAB_API_PORT", "8000"))
    reload = _parse_bool(os.getenv("CLAB_API_RELOAD", "false"))

    flags = FeatureFlags(
        enable_redis_cache=_parse_bool(os.getenv("CLAB_ENABLE_REDIS_CACHE", "false")),
        enable_duckdb_store=_parse_bool(os.getenv("CLAB_ENABLE_DUCKDB_STORE", "false")),
    )

    settings = Settings(
        env=env,
        storage=StorageSettings(
            root_dir=root_dir,
            bars_dir=bars_dir,
            features_dir=features_dir,
            runs_dir=runs_dir,
            artifacts_dir=artifacts_dir,
        ),
        flags=flags,
        api=ApiSettings(host=host, port=port, reload=reload),
    )

    _ensure_directories(settings.storage)
    return settings


def _ensure_directories(storage: StorageSettings) -> None:
    storage.root_dir.mkdir(parents=True, exist_ok=True)
    storage.bars_dir.mkdir(parents=True, exist_ok=True)
    storage.features_dir.mkdir(parents=True, exist_ok=True)
    storage.runs_dir.mkdir(parents=True, exist_ok=True)
    storage.artifacts_dir.mkdir(parents=True, exist_ok=True)


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}
