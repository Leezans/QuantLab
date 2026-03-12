from __future__ import annotations

from quantlab.app.services.job_bridge import EventJobSubscription
from quantlab.core.events import DomainEvent
from quantlab.core.jobs import JobExecutionMode, JobSpec
from quantlab.domain.events import (
    BacktestRequested,
    FactorComputationRequested,
    MarketDataDownloadRequested,
)


def _request_metadata(event: DomainEvent) -> dict[str, str]:
    correlation_id = event.correlation_id or event.event_id
    return {
        "requested_event_id": event.event_id,
        "correlation_id": correlation_id,
    }


def build_market_data_download_job(event: DomainEvent) -> JobSpec | None:
    if not isinstance(event, MarketDataDownloadRequested):
        return None

    return JobSpec(
        job_type="download.market_data",
        payload={
            "symbol": event.symbol,
            "start": event.start,
            "end": event.end,
            "provider": event.provider,
        },
        dedupe_key=f"download.market_data:{event.provider}:{event.symbol}:{event.start}:{event.end}",
        execution_mode=JobExecutionMode.THREAD,
        metadata=_request_metadata(event),
    )


def build_factor_computation_job(event: DomainEvent) -> JobSpec | None:
    if not isinstance(event, FactorComputationRequested):
        return None

    return JobSpec(
        job_type="compute.factor",
        payload={
            "factor_name": event.factor_name,
            "dataset_uri": event.dataset_uri,
            "parameters": dict(event.parameters),
        },
        dedupe_key=f"compute.factor:{event.factor_name}:{event.dataset_uri}:{sorted(event.parameters.items())}",
        execution_mode=JobExecutionMode.PROCESS,
        metadata=_request_metadata(event),
    )


def build_backtest_job(event: DomainEvent) -> JobSpec | None:
    if not isinstance(event, BacktestRequested):
        return None

    universe = tuple(event.universe)
    return JobSpec(
        job_type="run.backtest",
        payload={
            "strategy_name": event.strategy_name,
            "start": event.start,
            "end": event.end,
            "universe": universe,
            "parameters": dict(event.parameters),
        },
        dedupe_key=f"run.backtest:{event.strategy_name}:{event.start}:{event.end}:{universe}:{sorted(event.parameters.items())}",
        execution_mode=JobExecutionMode.PROCESS,
        metadata=_request_metadata(event),
    )


def default_event_job_subscriptions() -> tuple[EventJobSubscription, ...]:
    return (
        EventJobSubscription(
            event_type=MarketDataDownloadRequested,
            build_spec=build_market_data_download_job,
        ),
        EventJobSubscription(
            event_type=FactorComputationRequested,
            build_spec=build_factor_computation_job,
        ),
        EventJobSubscription(
            event_type=BacktestRequested,
            build_spec=build_backtest_job,
        ),
    )
