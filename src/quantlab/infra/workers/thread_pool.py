from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Final

from quantlab.app.services.job_service import JobService
from quantlab.core.interfaces import JobQueue, JobRegistry, WorkerPool


class ThreadPoolWorkerPool(WorkerPool):
    def __init__(
        self,
        queue: JobQueue,
        registry: JobRegistry,
        job_service: JobService,
        max_workers: int = 4,
        poll_timeout: float = 0.5,
    ) -> None:
        self._queue = queue
        self._registry = registry
        self._job_service = job_service
        self._max_workers: Final[int] = max_workers
        self._poll_timeout = poll_timeout

        self._executor: ThreadPoolExecutor | None = None
        self._dispatcher_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._executor is not None:
            return

        self._stop_event.clear()
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop,
            name="job-dispatcher",
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
            assert self._executor is not None
            self._executor.submit(self._run_one, job_id)

    def _run_one(self, job_id: str) -> None:
        try:
            job = self._job_service.mark_running(job_id)
            handler = self._registry.get(job.job_type)
            ctx = self._job_service.build_context(job_id)
            result = handler(job.payload, ctx)
            self._job_service.mark_succeeded(job_id, result)
        except Exception as exc:
            self._job_service.mark_failed(job_id, str(exc))