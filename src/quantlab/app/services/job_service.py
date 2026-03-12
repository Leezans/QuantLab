from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, UTC

from quantlab.core.events import (
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
                )

        job = JobRecord.create(spec)
        job.status = JobStatus.QUEUED
        self._repo.add(job)
        self._queue.put(job.job_id)
        self._bus.publish(JobQueued(job_id=job.job_id))

        return SubmitJobResult(
            job_id=job.job_id,
            created=True,
            status=job.status.value,
        )

    def mark_running(self, job_id: str) -> JobRecord:
        job = self._require(job_id)
        job.status = JobStatus.RUNNING
        job.started_at = utc_now()
        job.progress = 0.0
        self._repo.update(job)
        self._bus.publish(JobStarted(job_id=job.job_id))
        return job

    def mark_succeeded(self, job_id: str, result: dict) -> JobRecord:
        job = self._require(job_id)
        job.status = JobStatus.SUCCEEDED
        job.progress = 1.0
        job.result = result
        job.finished_at = utc_now()
        self._repo.update(job)
        self._bus.publish(JobSucceeded(job_id=job.job_id, result=result))
        return job

    def mark_failed(self, job_id: str, error: str) -> JobRecord:
        job = self._require(job_id)
        job.status = JobStatus.FAILED
        job.error = error
        job.finished_at = utc_now()
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

    def _require(self, job_id: str) -> JobRecord:
        job = self._repo.get(job_id)
        if job is None:
            raise KeyError(f"Job not found: {job_id}")
        return job