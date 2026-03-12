from __future__ import annotations

import time

from quantlab.app.bootstrap import build_async_task_runtime
from quantlab.config.models import QuantLabSettings, RuntimeSettings
from quantlab.core.jobs import JobExecutionMode, JobSpec


def fake_download_job(payload: dict, ctx) -> dict:
    symbol = payload["symbol"]
    start = payload["start"]
    end = payload["end"]

    total_steps = 5
    for step in range(total_steps):
        time.sleep(0.3)
        progress = (step + 1) / total_steps
        ctx.set_progress(progress, f"downloading {symbol} {start} {end}: step {step + 1}/{total_steps}")

    return {
        "symbol": symbol,
        "start": start,
        "end": end,
        "rows": 12345,
        "artifact_uri": f"local://datasets/{symbol}/{start}_{end}.parquet",
    }


def fake_backtest_job(payload: dict, ctx) -> dict:
    universe = ",".join(payload["universe"])
    return {
        "strategy_name": payload["strategy_name"],
        "period": f"{payload['start']}->{payload['end']}",
        "universe": universe,
        "sharpe": 1.42,
    }


def print_event(event) -> None:
    print(f"[event] type={event.event_type} job_id={getattr(event, 'job_id', '-')}")


def main() -> None:
    settings = QuantLabSettings(
        runtime=RuntimeSettings(
            max_workers=2,
            process_workers=1,
            queue_poll_timeout=0.1,
        )
    )
    runtime = build_async_task_runtime(settings)
    runtime.subscribe("*", print_event)
    runtime.register_job_handler("download.market_data", fake_download_job)
    runtime.register_job_handler("run.backtest", fake_backtest_job)

    runtime.start()

    download = runtime.submit_job(
        JobSpec(
            job_type="download.market_data",
            payload={
                "symbol": "BTCUSDT",
                "start": "2026-01-01",
                "end": "2026-01-31",
            },
            dedupe_key="download:BTCUSDT:2026-01-01:2026-01-31",
            execution_mode=JobExecutionMode.THREAD,
        )
    )
    duplicate = runtime.submit_job(
        JobSpec(
            job_type="download.market_data",
            payload={
                "symbol": "BTCUSDT",
                "start": "2026-01-01",
                "end": "2026-01-31",
            },
            dedupe_key="download:BTCUSDT:2026-01-01:2026-01-31",
            execution_mode=JobExecutionMode.THREAD,
        )
    )
    backtest = runtime.submit_job(
        JobSpec(
            job_type="run.backtest",
            payload={
                "strategy_name": "mean_reversion",
                "start": "2025-01-01",
                "end": "2025-12-31",
                "universe": ("BTCUSDT", "ETHUSDT"),
            },
            dedupe_key="backtest:mean_reversion:2025-01-01:2025-12-31",
            execution_mode=JobExecutionMode.PROCESS,
        )
    )

    print("download:", download)
    print("duplicate:", duplicate)
    print("backtest:", backtest)

    active_job_ids = {download.job_id, backtest.job_id}
    while active_job_ids:
        time.sleep(0.25)
        for job_id in list(active_job_ids):
            status = runtime.get_job_status(job_id)
            if status is None:
                active_job_ids.remove(job_id)
                continue

            print(
                f"[job] id={status.job_id} type={status.job_type} mode={status.execution_mode} "
                f"status={status.status} progress={status.progress:.0%} message={status.message}"
            )
            if status.status in {"succeeded", "failed"}:
                print("[job-final]", status.job_id, status.result, status.error)
                active_job_ids.remove(job_id)

    runtime.stop()


if __name__ == "__main__":
    main()
