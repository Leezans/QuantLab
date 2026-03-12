from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from quantlab.app.services.job_service import JobService
from quantlab.core.events import DomainEvent
from quantlab.core.interfaces import EventBus, EventHandler
from quantlab.core.jobs import JobSpec

JobSpecFactory = Callable[[DomainEvent], JobSpec | None]


@dataclass(frozen=True, slots=True)
class EventJobSubscription:
    event_type: str | type[DomainEvent]
    build_spec: JobSpecFactory


class EventToJobBridge(EventHandler):
    def __init__(self, job_service: JobService, build_spec: JobSpecFactory) -> None:
        self._job_service = job_service
        self._build_spec = build_spec

    def __call__(self, event: DomainEvent) -> None:
        spec = self._build_spec(event)
        if spec is None:
            return
        self._job_service.submit(spec)


def register_event_job_subscriptions(
    bus: EventBus,
    job_service: JobService,
    subscriptions: Iterable[EventJobSubscription],
) -> None:
    for subscription in subscriptions:
        bus.subscribe(
            subscription.event_type,
            EventToJobBridge(job_service=job_service, build_spec=subscription.build_spec),
        )
