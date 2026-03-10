from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class RawArtifactRef:
    vendor: str
    dataset_name: str
    symbol: str
    date: str
    data_path: Path
    checksum_path: Path | None = None
    manifest_path: Path | None = None
    source_url: str = ""
    checksum_url: str | None = None
    sha256: str = ""
    expected_sha256: str | None = None
    verified: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "vendor": self.vendor,
            "dataset_name": self.dataset_name,
            "symbol": self.symbol,
            "date": self.date,
            "data_path": str(self.data_path),
            "checksum_path": str(self.checksum_path) if self.checksum_path else None,
            "manifest_path": str(self.manifest_path) if self.manifest_path else None,
            "source_url": self.source_url,
            "checksum_url": self.checksum_url,
            "sha256": self.sha256,
            "expected_sha256": self.expected_sha256,
            "verified": self.verified,
            "created_at": self.created_at.isoformat(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "RawArtifactRef":
        checksum_path = payload.get("checksum_path")
        manifest_path = payload.get("manifest_path")
        return cls(
            vendor=str(payload["vendor"]),
            dataset_name=str(payload["dataset_name"]),
            symbol=str(payload["symbol"]),
            date=str(payload["date"]),
            data_path=Path(str(payload["data_path"])),
            checksum_path=Path(str(checksum_path)) if checksum_path else None,
            manifest_path=Path(str(manifest_path)) if manifest_path else None,
            source_url=str(payload.get("source_url", "")),
            checksum_url=str(payload["checksum_url"]) if payload.get("checksum_url") else None,
            sha256=str(payload.get("sha256", "")),
            expected_sha256=str(payload["expected_sha256"]) if payload.get("expected_sha256") else None,
            verified=bool(payload.get("verified", False)),
            created_at=datetime.fromisoformat(str(payload.get("created_at", datetime.now(timezone.utc).isoformat()))),
            metadata={str(key): str(value) for key, value in dict(payload.get("metadata", {})).items()},
        )


class RawArtifactStore:
    """Stores immutable vendor raw files and a sidecar manifest for auditability."""

    def __init__(self, base_path: Path) -> None:
        self._base_path = base_path

    @property
    def base_path(self) -> Path:
        return self._base_path

    def write_bytes_atomic(self, relative_path: Path, payload: bytes) -> Path:
        destination = self._base_path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=destination.parent, delete=False) as temp_file:
            temp_file.write(payload)
            temp_path = Path(temp_file.name)
        temp_path.replace(destination)
        return destination

    def write_text_atomic(self, relative_path: Path, payload: str, encoding: str = "utf-8") -> Path:
        return self.write_bytes_atomic(relative_path, payload.encode(encoding))

    def register(self, artifact: RawArtifactRef) -> RawArtifactRef:
        manifest_relative_path = self.manifest_relative_path(artifact.data_path.relative_to(self._base_path))
        manifest_path = self._base_path / manifest_relative_path
        materialized = RawArtifactRef(
            vendor=artifact.vendor,
            dataset_name=artifact.dataset_name,
            symbol=artifact.symbol,
            date=artifact.date,
            data_path=artifact.data_path,
            checksum_path=artifact.checksum_path,
            manifest_path=manifest_path,
            source_url=artifact.source_url,
            checksum_url=artifact.checksum_url,
            sha256=artifact.sha256,
            expected_sha256=artifact.expected_sha256,
            verified=artifact.verified,
            created_at=artifact.created_at,
            metadata=artifact.metadata,
        )
        self.write_text_atomic(manifest_relative_path, json.dumps(materialized.to_dict(), indent=2))
        return materialized

    def load(self, relative_data_path: Path) -> RawArtifactRef | None:
        manifest_path = self._base_path / self.manifest_relative_path(relative_data_path)
        if not manifest_path.exists():
            return None
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        return RawArtifactRef.from_dict(payload)

    def manifest_relative_path(self, relative_data_path: Path) -> Path:
        return relative_data_path.with_name(relative_data_path.name + ".manifest.json")
