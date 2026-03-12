from __future__ import annotations

from threading import RLock

from quantlab.core.jobs import JobRecord, JobStatus
from quantlab.core.interfaces import JobRepository, JobRegistry, JobHandler


class InMemoryJobRepository(JobRepository):
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = RLock()

    def add(self, job: JobRecord) -> None:
        with self._lock:
            self._jobs[job.job_id] = job

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def update(self, job: JobRecord) -> None:
        with self._lock:
            self._jobs[job.job_id] = job

    def find_active_by_dedupe_key(self, dedupe_key: str) -> JobRecord | None:
        with self._lock:
            for job in self._jobs.values():
                if job.dedupe_key != dedupe_key:
                    continue
                if job.status in {JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING}:
                    return job
        return None


class InMemoryJobRegistry(JobRegistry):
    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_type: str, handler: JobHandler) -> None:
        self._handlers[job_type] = handler

    def get(self, job_type: str) -> JobHandler:
        try:
            return self._handlers[job_type]
        except KeyError as exc:
            raise KeyError(f"No handler registered for job_type={job_type!r}") from exc