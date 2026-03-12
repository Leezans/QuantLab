from __future__ import annotations

import threading
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any, Final

from quantlab.app.services.job_service import JobService
from quantlab.core.interfaces import JobContext, JobQueue, JobRegistry, JobRepository, WorkerPool


class NoOpJobContext(JobContext):
    def set_progress(self, progress: float, message: str = "") -> None:
        return


def run_process_job(handler: Any, payload: dict[str, Any]) -> dict[str, Any]:
    return handler(payload, NoOpJobContext())


class ProcessPoolWorkerPool(WorkerPool):
    """
    Single-mode process worker pool.

    Use HybridWorkerPool when a queue may contain both thread and process jobs.
    """

    def __init__(
        self,
        queue: JobQueue,
        repo: JobRepository,
        registry: JobRegistry,
        job_service: JobService,
        max_workers: int = 2,
        poll_timeout: float = 0.5,
    ) -> None:
        self._queue = queue
        self._repo = repo
        self._registry = registry
        self._job_service = job_service
        self._max_workers: Final[int] = max_workers
        self._poll_timeout = poll_timeout

        self._executor: ProcessPoolExecutor | None = None
        self._dispatcher_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._dispatcher_thread is not None:
            return

        self._stop_event.clear()
        self._executor = ProcessPoolExecutor(max_workers=self._max_workers)
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop,
            name="process-job-dispatcher",
            daemon=True,
        )
        self._dispatcher_thread.start()

    def stop(self) -> None:
        self._stop_event.set()

        if self._dispatcher_thread is not None:
            self._dispatcher_thread.join(timeout=2.0)
            self._dispatcher_thread = None

        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None

    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            job_id = self._queue.get(timeout=self._poll_timeout)
            if job_id is None:
                continue

            job = self._repo.get(job_id)
            if job is None:
                continue

            try:
                self._job_service.mark_running(job_id)
                handler = self._registry.get(job.job_type)
                assert self._executor is not None
                future = self._executor.submit(run_process_job, handler, job.payload)
                future.add_done_callback(
                    lambda completed, current_job_id=job_id: self._complete_job(current_job_id, completed)
                )
            except Exception as exc:
                self._job_service.mark_failed(job_id, str(exc))

    def _complete_job(self, job_id: str, future: Future[dict[str, Any]]) -> None:
        try:
            result = future.result()
        except Exception as exc:
            self._job_service.mark_failed(job_id, str(exc))
            return

        self._job_service.mark_succeeded(job_id, result)
