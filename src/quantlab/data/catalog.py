from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Mapping

from quantlab.core.enums import AssetClass, DatasetKind, StorageTier


@dataclass(frozen=True, slots=True)
class DatasetRef:
    name: str
    version: str
    data_kind: DatasetKind
    asset_class: AssetClass
    location: Path
    schema: tuple[str, ...]
    storage_tier: StorageTier = StorageTier.NORMALIZED
    row_count: int = 0
    format: str = "parquet"
    partition_columns: tuple[str, ...] = field(default_factory=tuple)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "data_kind": self.data_kind.value,
            "storage_tier": self.storage_tier.value,
            "asset_class": self.asset_class.value,
            "location": str(self.location),
            "schema": list(self.schema),
            "row_count": self.row_count,
            "format": self.format,
            "partition_columns": list(self.partition_columns),
            "created_at": self.created_at.isoformat(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "DatasetRef":
        return cls(
            name=str(payload["name"]),
            version=str(payload["version"]),
            data_kind=DatasetKind(str(payload.get("data_kind", DatasetKind.BAR.value))),
            storage_tier=StorageTier(str(payload.get("storage_tier", StorageTier.NORMALIZED.value))),
            asset_class=AssetClass(str(payload["asset_class"])),
            location=Path(str(payload["location"])),
            schema=tuple(str(value) for value in payload.get("schema", ())),
            row_count=int(payload.get("row_count", 0)),
            format=str(payload.get("format", "parquet")),
            partition_columns=tuple(str(value) for value in payload.get("partition_columns", ())),
            created_at=datetime.fromisoformat(str(payload.get("created_at", datetime.now(timezone.utc).isoformat()))),
            metadata={str(key): str(value) for key, value in dict(payload.get("metadata", {})).items()},
        )


class DataCatalog:
    def __init__(self, catalog_path: Path | None = None) -> None:
        self._catalog_path = catalog_path
        self._datasets: dict[tuple[str, str], DatasetRef] = {}
        if self._catalog_path and self._catalog_path.exists():
            self.load()

    def register(self, dataset: DatasetRef) -> None:
        with self._locked():
            self._sync_from_disk()
            key = (dataset.name, dataset.version)
            self._datasets[key] = dataset
            self.save()

    def resolve(self, name: str, version: str = "latest") -> DatasetRef:
        if version != "latest":
            return self._datasets[(name, version)]

        matches = [dataset for (dataset_name, _), dataset in self._datasets.items() if dataset_name == name]
        if not matches:
            raise KeyError(f"dataset not found: {name}")
        return sorted(matches, key=lambda dataset: dataset.created_at)[-1]

    def get(self, name: str, version: str) -> DatasetRef | None:
        return self._datasets.get((name, version))

    def unregister(self, name: str, version: str) -> DatasetRef | None:
        with self._locked():
            self._sync_from_disk()
            dataset = self._datasets.pop((name, version), None)
            self.save()
            return dataset

    def list(self) -> tuple[DatasetRef, ...]:
        return tuple(sorted(self._datasets.values(), key=lambda dataset: (dataset.name, dataset.created_at, dataset.version)))

    def save(self) -> None:
        if self._catalog_path is None:
            return
        self._catalog_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"datasets": [dataset.to_dict() for dataset in self.list()]}
        with NamedTemporaryFile("w", dir=self._catalog_path.parent, delete=False, encoding="utf-8") as handle:
            handle.write(json.dumps(payload, indent=2))
            temp_path = Path(handle.name)
        temp_path.replace(self._catalog_path)

    def load(self) -> None:
        if self._catalog_path is None or not self._catalog_path.exists():
            return
        payload = json.loads(self._catalog_path.read_text(encoding="utf-8"))
        self._datasets = {}
        for record in payload.get("datasets", []):
            dataset = DatasetRef.from_dict(record)
            self._datasets[(dataset.name, dataset.version)] = dataset

    def _sync_from_disk(self) -> None:
        if self._catalog_path is None or not self._catalog_path.exists():
            return
        payload = json.loads(self._catalog_path.read_text(encoding="utf-8"))
        for record in payload.get("datasets", []):
            dataset = DatasetRef.from_dict(record)
            self._datasets[(dataset.name, dataset.version)] = dataset

    @contextmanager
    def _locked(self):
        if self._catalog_path is None:
            yield
            return
        lock_path = self._catalog_path.with_suffix(self._catalog_path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        start = time.monotonic()
        fd: int | None = None
        while fd is None:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except FileExistsError:
                if time.monotonic() - start > 10.0:
                    raise TimeoutError(f"timed out waiting for catalog lock: {lock_path}")
                time.sleep(0.05)
        try:
            yield
        finally:
            if fd is not None:
                os.close(fd)
            if lock_path.exists():
                lock_path.unlink()
