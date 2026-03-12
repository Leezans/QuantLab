from __future__ import annotations

import time

from quantlab.app.services.job_runner import DefaultJobRunner
from quantlab.app.services.job_service import JobService
from quantlab.core.jobs import JobSpec
from quantlab.infra.bus.in_memory import InMemoryEventBus
from quantlab.infra.jobs.in_memory import InMemoryJobRegistry, InMemoryJobRepository
from quantlab.infra.queue.in_memory import InMemoryJobQueue
from quantlab.infra.workers.thread_pool import ThreadPoolWorkerPool


def fake_download_job(payload: dict, ctx) -> dict:
    symbol = payload["symbol"]
    start = payload["start"]
    end = payload["end"]

    total_steps = 5
    for step in range(total_steps):
        time.sleep(0.6)
        progress = (step + 1) / total_steps
        ctx.set_progress(progress, f"downloading {symbol} {start} {end}: step {step + 1}/{total_steps}")

    return {
        "symbol": symbol,
        "start": start,
        "end": end,
        "rows": 12345,
        "artifact_uri": f"local://datasets/{symbol}/{start}_{end}.parquet",
    }


def print_event(event) -> None:
    print(f"[event] type={event.event_type} job_id={getattr(event, 'job_id', '-')}")


def main() -> None:
    bus = InMemoryEventBus()
    repo = InMemoryJobRepository()
    queue = InMemoryJobQueue()
    registry = InMemoryJobRegistry()
    service = JobService(repo=repo, queue=queue, bus=bus)
    job_runner = DefaultJobRunner(registry=registry, job_service=service)
    workers = ThreadPoolWorkerPool(
        queue=queue,
        job_runner=job_runner,
        max_workers=2,
    )

    bus.subscribe("*", print_event)
    registry.register("download.market_data", fake_download_job)

    workers.start()

    result_1 = service.submit(
        JobSpec(
            job_type="download.market_data",
            payload={
                "symbol": "BTCUSDT",
                "start": "2026-01-01",
                "end": "2026-01-31",
            },
            dedupe_key="download:BTCUSDT:2026-01-01:2026-01-31",
        )
    )

    result_2 = service.submit(
        JobSpec(
            job_type="download.market_data",
            payload={
                "symbol": "BTCUSDT",
                "start": "2026-01-01",
                "end": "2026-01-31",
            },
            dedupe_key="download:BTCUSDT:2026-01-01:2026-01-31",
        )
    )

    print("submit result 1:", result_1)
    print("submit result 2:", result_2)

    while True:
        job = service.get_job(result_1.job_id)
        if job is None:
            break

        print(
            f"[job] id={job.job_id} status={job.status.value} "
            f"progress={job.progress:.2%} message={job.message}"
        )

        if job.status.value in {"succeeded", "failed"}:
            print("[job-final]", job.result, job.error)
            break

        time.sleep(0.5)

    workers.stop()


if __name__ == "__main__":
    main()
