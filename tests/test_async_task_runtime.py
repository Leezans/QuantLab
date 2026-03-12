from __future__ import annotations

import time

from quantlab.app.bootstrap import build_async_task_runtime
from quantlab.app.events import JobQueued
from quantlab.app.services.job_service import JobService
from quantlab.config.models import QuantLabSettings, RuntimeSettings
from quantlab.core.jobs import JobExecutionMode, JobSpec
from quantlab.domain.events import MarketDataDownloadRequested
from quantlab.infra.bus.in_memory import InMemoryEventBus
from quantlab.infra.jobs.in_memory import InMemoryJobRepository
from quantlab.infra.queue.in_memory import InMemoryJobQueue


def thread_download_job(payload: dict, ctx) -> dict:
    ctx.set_progress(0.5, "halfway")
    return {
        "artifact_uri": f"local://datasets/{payload['symbol']}/{payload['start']}_{payload['end']}.parquet"
    }


def process_backtest_job(payload: dict, ctx) -> dict:
    score = sum(value * value for value in payload["values"])
    return {
        "strategy_name": payload["strategy_name"],
        "score": score,
    }


def _wait_until(predicate, timeout: float = 10.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("timed out waiting for condition")


def _wait_for_terminal_status(runtime, job_id: str, timeout: float = 10.0):
    def load_status():
        status = runtime.get_job_status(job_id)
        if status is None:
            return None
        if status.status in {"succeeded", "failed"}:
            return status
        return None

    return _wait_until(load_status, timeout=timeout)


def test_job_service_deduplicates_active_jobs() -> None:
    service = JobService(
        repo=InMemoryJobRepository(),
        queue=InMemoryJobQueue(),
        bus=InMemoryEventBus(),
    )

    first = service.submit(
        JobSpec(
            job_type="download.market_data",
            payload={"symbol": "BTCUSDT"},
            dedupe_key="download:BTCUSDT",
        )
    )
    second = service.submit(
        JobSpec(
            job_type="download.market_data",
            payload={"symbol": "BTCUSDT"},
            dedupe_key="download:BTCUSDT",
        )
    )

    assert first.created is True
    assert second.created is False
    assert second.job_id == first.job_id
    assert second.status == "queued"


def test_domain_event_can_enqueue_and_complete_thread_job() -> None:
    queued_job_ids: list[str] = []
    runtime = build_async_task_runtime(
        QuantLabSettings(
            runtime=RuntimeSettings(
                max_workers=1,
                process_workers=1,
                queue_poll_timeout=0.05,
            )
        )
    )
    runtime.register_job_handler("download.market_data", thread_download_job)
    runtime.subscribe(JobQueued, lambda event: queued_job_ids.append(event.job_id))

    runtime.start()
    try:
        runtime.publish(
            MarketDataDownloadRequested(
                symbol="BTCUSDT",
                start="2026-01-01",
                end="2026-01-31",
            )
        )

        job_id = _wait_until(lambda: queued_job_ids[0] if queued_job_ids else None)
        status = _wait_for_terminal_status(runtime, job_id)

        assert status.status == "succeeded"
        assert status.execution_mode == "thread"
        assert status.result == {
            "artifact_uri": "local://datasets/BTCUSDT/2026-01-01_2026-01-31.parquet"
        }
    finally:
        runtime.stop()


def test_runtime_routes_process_jobs_to_process_pool() -> None:
    runtime = build_async_task_runtime(
        QuantLabSettings(
            runtime=RuntimeSettings(
                max_workers=1,
                process_workers=1,
                queue_poll_timeout=0.05,
            )
        )
    )
    runtime.register_job_handler("run.backtest", process_backtest_job)

    runtime.start()
    try:
        submission = runtime.submit_job(
            JobSpec(
                job_type="run.backtest",
                payload={
                    "strategy_name": "demo",
                    "values": [1, 2, 3, 4],
                },
                dedupe_key="backtest:demo",
                execution_mode=JobExecutionMode.PROCESS,
            )
        )

        status = _wait_for_terminal_status(runtime, submission.job_id, timeout=15.0)

        assert status.status == "succeeded"
        assert status.execution_mode == "process"
        assert status.result == {
            "strategy_name": "demo",
            "score": 30,
        }
    finally:
        runtime.stop()


if __name__ == "__main__":
    test_job_service_deduplicates_active_jobs()
    test_domain_event_can_enqueue_and_complete_thread_job()
    test_runtime_routes_process_jobs_to_process_pool()
    print("manual tests passed")
