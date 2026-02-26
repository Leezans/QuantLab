from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cLab.core.data.protocols import ExperimentStore
from cLab.core.domain.errors import DataNotFoundError


@dataclass(frozen=True)
class ResearchPipeline:
    experiment_store: ExperimentStore

    def get_run(self, run_id: str) -> dict[str, Any]:
        record = self.experiment_store.get_run(run_id)
        if record is None:
            raise DataNotFoundError(f"run_id not found: {run_id}")
        return record

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.experiment_store.list_runs(limit=limit)

