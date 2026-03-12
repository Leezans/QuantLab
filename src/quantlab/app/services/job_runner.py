from __future__ import annotations

from quantlab.app.services.job_service import JobService
from quantlab.core.interfaces import JobRegistry, JobRunner


class DefaultJobRunner(JobRunner):
    def __init__(self, registry: JobRegistry, job_service: JobService) -> None:
        self._registry = registry
        self._job_service = job_service

    def run(self, job_id: str) -> None:
        try:
            job = self._job_service.mark_running(job_id)
            handler = self._registry.get(job.job_type)
            ctx = self._job_service.build_context(job_id)
            result = handler(job.payload, ctx)
            self._job_service.mark_succeeded(job_id, result)
        except Exception as exc:
            self._job_service.mark_failed(job_id, str(exc))
