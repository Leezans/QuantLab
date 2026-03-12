from __future__ import annotations

import threading
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Final

from quantlab.app.services.job_runner import DefaultJobRunner
from quantlab.app.services.job_service import JobService
from quantlab.core.jobs import JobExecutionMode
from quantlab.core.interfaces import JobQueue, JobRegistry, JobRepository, WorkerPool
from quantlab.infra.workers.process_pool import run_process_job


class HybridWorkerPool(WorkerPool):
    def __init__(
        self,
        queue: JobQueue,
        repo: JobRepository,
        registry: JobRegistry,
        job_service: JobService,
        thread_workers: int = 4,
        process_workers: int = 2,
        poll_timeout: float = 0.5,
    ) -> None:
        self._queue = queue
        self._repo = repo
        self._registry = registry
        self._job_service = job_service
        self._thread_runner = DefaultJobRunner(registry=registry, job_service=job_service)
        self._thread_workers: Final[int] = thread_workers
        self._process_workers: Final[int] = process_workers
        self._poll_timeout = poll_timeout

        self._thread_executor: ThreadPoolExecutor | None = None
        self._process_executor: ProcessPoolExecutor | None = None
        self._dispatcher_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._dispatcher_thread is not None:
            return

        self._stop_event.clear()
        self._thread_executor = ThreadPoolExecutor(max_workers=self._thread_workers)
        if self._process_workers > 0:
            self._process_executor = ProcessPoolExecutor(max_workers=self._process_workers)
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop,
            name="hybrid-job-dispatcher",
            daemon=True,
        )
        self._dispatcher_thread.start()

    def stop(self) -> None:
        self._stop_event.set()

        if self._dispatcher_thread is not None:
            self._dispatcher_thread.join(timeout=2.0)
            self._dispatcher_thread = None

        if self._thread_executor is not None:
            self._thread_executor.shutdown(wait=True, cancel_futures=False)
            self._thread_executor = None

        if self._process_executor is not None:
            self._process_executor.shutdown(wait=True, cancel_futures=False)
            self._process_executor = None

    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            job_id = self._queue.get(timeout=self._poll_timeout)
            if job_id is None:
                continue

            job = self._repo.get(job_id)
            if job is None or not job.is_active:
                continue

            if job.execution_mode == JobExecutionMode.PROCESS:
                self._submit_process_job(job.job_id)
                continue

            assert self._thread_executor is not None
            self._thread_executor.submit(self._thread_runner.run, job.job_id)

    def _submit_process_job(self, job_id: str) -> None:
        job = self._repo.get(job_id)
        if job is None:
            return

        if self._process_executor is None:
            self._job_service.mark_failed(job_id, "Process worker pool is not configured")
            return

        try:
            self._job_service.mark_running(job_id)
            handler = self._registry.get(job.job_type)
            future = self._process_executor.submit(run_process_job, handler, job.payload)
            future.add_done_callback(
                lambda completed, current_job_id=job_id: self._complete_process_job(current_job_id, completed)
            )
        except Exception as exc:
            self._job_service.mark_failed(job_id, str(exc))

    def _complete_process_job(self, job_id: str, future: Future[dict[str, Any]]) -> None:
        try:
            result = future.result()
        except Exception as exc:
            self._job_service.mark_failed(job_id, str(exc))
            return

        self._job_service.mark_succeeded(job_id, result)
