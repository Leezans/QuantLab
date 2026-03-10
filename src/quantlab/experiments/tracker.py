from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class ExperimentRun:
    name: str
    started_at: datetime
    parameters: Mapping[str, Any]
    metrics: Mapping[str, float] = field(default_factory=dict)
    notes: str = ""
    run_id: str = field(default_factory=lambda: str(uuid4()))


class LocalExperimentTracker:
    def __init__(self, artifact_dir: Path) -> None:
        self._artifact_dir = artifact_dir

    def log_run(self, run: ExperimentRun) -> Path:
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        output_path = self._artifact_dir / f"{run.run_id}.json"
        payload = asdict(run)
        payload["started_at"] = run.started_at.isoformat()
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return output_path

    def load_run(self, run_id: str) -> ExperimentRun:
        payload = json.loads((self._artifact_dir / f"{run_id}.json").read_text(encoding="utf-8"))
        return ExperimentRun(
            name=str(payload["name"]),
            started_at=datetime.fromisoformat(str(payload["started_at"])),
            parameters=dict(payload.get("parameters", {})),
            metrics={str(key): float(value) for key, value in dict(payload.get("metrics", {})).items()},
            notes=str(payload.get("notes", "")),
            run_id=str(payload.get("run_id", run_id)),
        )

    def list_runs(self) -> tuple[ExperimentRun, ...]:
        if not self._artifact_dir.exists():
            return ()
        runs = [
            self.load_run(path.stem)
            for path in sorted(self._artifact_dir.glob("*.json"))
        ]
        return tuple(sorted(runs, key=lambda run: run.started_at, reverse=True))

    def compare_runs(self, metric: str) -> tuple[ExperimentRun, ...]:
        return tuple(
            sorted(
                self.list_runs(),
                key=lambda run: run.metrics.get(metric, float("-inf")),
                reverse=True,
            )
        )
