from __future__ import annotations

from quantlab.app.job_bindings import default_event_job_subscriptions
from quantlab.app.runtime import AsyncTaskRuntime
from quantlab.app.services.job_service import JobService
from quantlab.config.models import QuantLabSettings, ResearchSettings
from quantlab.domain.events import FeatureCalculated, MarketDataArrived
from quantlab.domain.research.handlers.feature_handler import FeatureCalculationHandler
from quantlab.domain.research.handlers.signal_handler import SignalGenerationHandler
from quantlab.infra.bus import (
    ExceptionMiddleware,
    InMemoryEventBus,
    LoggingMiddleware,
    SubscriptionRegistry,
    TimingMiddleware,
)
from quantlab.infra.jobs.in_memory import InMemoryJobRegistry, InMemoryJobRepository
from quantlab.infra.queue.in_memory import InMemoryJobQueue
from quantlab.infra.workers.hybrid_pool import HybridWorkerPool


def build_bus() -> InMemoryEventBus:
    registry = SubscriptionRegistry()
    return InMemoryEventBus(
        registry=registry,
        middlewares=[
            ExceptionMiddleware(),
            LoggingMiddleware(),
            TimingMiddleware(),
        ],
    )


def register_research_handlers(bus: InMemoryEventBus, settings: ResearchSettings) -> None:
    feature_handler = FeatureCalculationHandler(bus)
    signal_handler = SignalGenerationHandler(bus, threshold=settings.signal_threshold)

    bus.subscribe(MarketDataArrived, feature_handler)
    bus.subscribe(FeatureCalculated, signal_handler)


def build_async_task_runtime(settings: QuantLabSettings) -> AsyncTaskRuntime:
    bus = build_bus()
    register_research_handlers(bus, settings.research)

    repo = InMemoryJobRepository()
    queue = InMemoryJobQueue()
    registry = InMemoryJobRegistry()
    job_service = JobService(repo=repo, queue=queue, bus=bus)
    worker_pool = HybridWorkerPool(
        queue=queue,
        repo=repo,
        registry=registry,
        job_service=job_service,
        thread_workers=settings.runtime.max_workers,
        process_workers=settings.runtime.process_workers,
        poll_timeout=settings.runtime.queue_poll_timeout,
    )

    runtime = AsyncTaskRuntime(
        bus=bus,
        job_service=job_service,
        job_registry=registry,
        worker_pool=worker_pool,
    )
    runtime.register_event_jobs(default_event_job_subscriptions())
    return runtime
