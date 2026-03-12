from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol

from quantlab.core.events import DomainEvent
from quantlab.core.jobs import JobRecord, JobSpec


class EventHandler(Protocol):
    def __call__(self, event: DomainEvent) -> None: ...


class EventBus(ABC):
    @abstractmethod
    def subscribe(self, event_type: str | type[DomainEvent], handler: EventHandler) -> None:
        raise NotImplementedError

    @abstractmethod
    def publish(self, event: DomainEvent) -> None:
        raise NotImplementedError


class JobRepository(ABC):
    @abstractmethod
    def add(self, job: JobRecord) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, job_id: str) -> JobRecord | None:
        raise NotImplementedError

    @abstractmethod
    def update(self, job: JobRecord) -> None:
        raise NotImplementedError

    @abstractmethod
    def find_active_by_dedupe_key(self, dedupe_key: str) -> JobRecord | None:
        raise NotImplementedError


class JobQueue(ABC):
    @abstractmethod
    def put(self, job_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, timeout: float | None = None) -> str | None:
        raise NotImplementedError


class JobContext(ABC):
    @abstractmethod
    def set_progress(self, progress: float, message: str = "") -> None:
        raise NotImplementedError


class JobHandler(Protocol):
    def __call__(self, payload: dict, ctx: JobContext) -> dict: ...


class JobRegistry(ABC):
    @abstractmethod
    def register(self, job_type: str, handler: JobHandler) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, job_type: str) -> JobHandler:
        raise NotImplementedError


class WorkerPool(ABC):
    @abstractmethod
    def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError
