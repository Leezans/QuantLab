from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from quantlab.app.services.job_bridge import EventJobSubscription, register_event_job_subscriptions
from quantlab.app.services.job_service import JobService, JobStatusView, SubmitJobResult
from quantlab.core.events import DomainEvent
from quantlab.core.interfaces import EventBus, EventHandler, JobHandler, JobRegistry, WorkerPool
from quantlab.core.jobs import JobRecord, JobSpec


@dataclass(slots=True)
class AsyncTaskRuntime:
    bus: EventBus
    job_service: JobService
    job_registry: JobRegistry
    worker_pool: WorkerPool

    def start(self) -> None:
        self.worker_pool.start()

    def stop(self) -> None:
        self.worker_pool.stop()

    def submit_job(self, spec: JobSpec) -> SubmitJobResult:
        return self.job_service.submit(spec)

    def get_job(self, job_id: str) -> JobRecord | None:
        return self.job_service.get_job(job_id)

    def get_job_status(self, job_id: str) -> JobStatusView | None:
        return self.job_service.get_status(job_id)

    def register_job_handler(self, job_type: str, handler: JobHandler) -> None:
        self.job_registry.register(job_type, handler)

    def register_event_jobs(self, subscriptions: Iterable[EventJobSubscription]) -> None:
        register_event_job_subscriptions(
            bus=self.bus,
            job_service=self.job_service,
            subscriptions=subscriptions,
        )

    def publish(self, event: DomainEvent) -> None:
        self.bus.publish(event)

    def subscribe(self, event_type: str | type[DomainEvent], handler: EventHandler) -> None:
        self.bus.subscribe(event_type, handler)

    def __enter__(self) -> "AsyncTaskRuntime":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()
