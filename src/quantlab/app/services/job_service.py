from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any

from quantlab.app.events import (
    JobFailed,
    JobProgressed,
    JobQueued,
    JobStarted,
    JobSucceeded,
)
from quantlab.core.jobs import JobRecord, JobSpec, JobStatus
from quantlab.core.interfaces import EventBus, JobContext, JobQueue, JobRepository


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class SubmitJobResult:
    job_id: str
    created: bool
    status: str
    job_type: str
    execution_mode: str


@dataclass(frozen=True, slots=True)
class JobStatusView:
    job_id: str
    job_type: str
    status: str
    execution_mode: str
    progress: float
    message: str
    result: dict[str, Any] | None
    error: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    @classmethod
    def from_record(cls, job: JobRecord) -> "JobStatusView":
        return cls(
            job_id=job.job_id,
            job_type=job.job_type,
            status=job.status.value,
            execution_mode=job.execution_mode.value,
            progress=job.progress,
            message=job.message,
            result=job.result,
            error=job.error,
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
        )


class DefaultJobContext(JobContext):
    def __init__(
        self,
        job_id: str,
        repo: JobRepository,
        bus: EventBus,
    ) -> None:
        self._job_id = job_id
        self._repo = repo
        self._bus = bus

    def set_progress(self, progress: float, message: str = "") -> None:
        job = self._repo.get(self._job_id)
        if job is None:
            return
        job.progress = max(0.0, min(1.0, progress))
        job.message = message
        job.updated_at = utc_now()
        self._repo.update(job)
        self._bus.publish(
            JobProgressed(
                job_id=self._job_id,
                progress=job.progress,
                message=message,
            )
        )


class JobService:
    def __init__(
        self,
        repo: JobRepository,
        queue: JobQueue,
        bus: EventBus,
    ) -> None:
        self._repo = repo
        self._queue = queue
        self._bus = bus

    def submit(self, spec: JobSpec) -> SubmitJobResult:
        if spec.dedupe_key:
            existing = self._repo.find_active_by_dedupe_key(spec.dedupe_key)
            if existing is not None:
                return SubmitJobResult(
                    job_id=existing.job_id,
                    created=False,
                    status=existing.status.value,
                    job_type=existing.job_type,
                    execution_mode=existing.execution_mode.value,
                )

        job = JobRecord.create(spec)
        job.status = JobStatus.QUEUED
        job.message = "queued"
        job.updated_at = utc_now()
        self._repo.add(job)
        self._queue.put(job.job_id)
        self._bus.publish(JobQueued(job_id=job.job_id))

        return SubmitJobResult(
            job_id=job.job_id,
            created=True,
            status=job.status.value,
            job_type=job.job_type,
            execution_mode=job.execution_mode.value,
        )

    def mark_running(self, job_id: str) -> JobRecord:
        job = self._require(job_id)
        job.status = JobStatus.RUNNING
        job.started_at = utc_now()
        job.updated_at = job.started_at
        job.progress = 0.0
        job.message = "running"
        self._repo.update(job)
        self._bus.publish(JobStarted(job_id=job.job_id))
        return job

    def mark_succeeded(self, job_id: str, result: dict) -> JobRecord:
        job = self._require(job_id)
        job.status = JobStatus.SUCCEEDED
        job.progress = 1.0
        job.result = result
        job.message = "completed"
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        self._repo.update(job)
        self._bus.publish(JobSucceeded(job_id=job.job_id, result=result))
        return job

    def mark_failed(self, job_id: str, error: str) -> JobRecord:
        job = self._require(job_id)
        job.status = JobStatus.FAILED
        job.error = error
        job.message = error
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        self._repo.update(job)
        self._bus.publish(JobFailed(job_id=job.job_id, error=error))
        return job

    def build_context(self, job_id: str) -> JobContext:
        return DefaultJobContext(
            job_id=job_id,
            repo=self._repo,
            bus=self._bus,
        )

    def get_job(self, job_id: str) -> JobRecord | None:
        return self._repo.get(job_id)

    def get_status(self, job_id: str) -> JobStatusView | None:
        job = self.get_job(job_id)
        if job is None:
            return None
        return JobStatusView.from_record(job)

    def _require(self, job_id: str) -> JobRecord:
        job = self._repo.get(job_id)
        if job is None:
            raise KeyError(f"Job not found: {job_id}")
        return job
