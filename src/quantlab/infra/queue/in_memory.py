from __future__ import annotations

from queue import Empty, Queue

from quantlab.core.interfaces import JobQueue


class InMemoryJobQueue(JobQueue):
    def __init__(self) -> None:
        self._queue: Queue[str] = Queue()

    def put(self, job_id: str) -> None:
        self._queue.put(job_id)

    def get(self, timeout: float | None = None) -> str | None:
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            return None