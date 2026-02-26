from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import uuid4

from cLab.core.data.protocols import ExperimentStore, MarketDataStore
from cLab.core.domain.errors import DataNotFoundError, ValidationError
from cLab.core.domain.types import BacktestMetrics, DateRange, RunRecord, StrategyParams
from cLab.core.experiments.record import serialize_run_record, utc_now
from cLab.modules.trading import run_ma_crossover_backtest


@dataclass(frozen=True)
class BacktestRunCommand:
    symbol: str
    start: date
    end: date
    strategy_params: StrategyParams
    interval: str = "1h"
    market: str = "spot"
    style: str = "mirror"
    fee_bps: float = 2.0
    slippage_bps: float = 1.0
    initial_cash: float = 10_000.0
    seed: int | None = None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValidationError("symbol is empty")
        if self.end < self.start:
            raise ValidationError("end < start")
        if self.initial_cash <= 0:
            raise ValidationError("initial_cash must be positive")
        if self.fee_bps < 0 or self.slippage_bps < 0:
            raise ValidationError("fee_bps/slippage_bps must be >= 0")


@dataclass(frozen=True)
class BacktestRunResult:
    run_id: str
    metrics: BacktestMetrics
    artifact_path: str


@dataclass(frozen=True)
class BacktestPipeline:
    market_data_store: MarketDataStore
    experiment_store: ExperimentStore

    def run(self, command: BacktestRunCommand) -> BacktestRunResult:
        bars = self.market_data_store.load_bars(
            symbol=command.symbol,
            start=command.start,
            end=command.end,
            interval=command.interval,
            market=command.market,
            style=command.style,
        )
        if bars.empty:
            raise DataNotFoundError(
                f"No bars loaded for {command.symbol.upper()} [{command.start}, {command.end}]",
            )

        result = run_ma_crossover_backtest(
            bars=bars,
            strategy_params=command.strategy_params,
            fee_bps=command.fee_bps,
            slippage_bps=command.slippage_bps,
            initial_cash=command.initial_cash,
        )

        run_id = uuid4().hex
        run_record = RunRecord(
            run_id=run_id,
            symbol=command.symbol.upper(),
            date_range=DateRange(start=command.start, end=command.end),
            strategy_params=command.strategy_params,
            fee_bps=command.fee_bps,
            slippage_bps=command.slippage_bps,
            initial_cash=command.initial_cash,
            seed=command.seed,
            metrics=result.metrics,
            artifact_path="",
            created_at=utc_now(),
        )
        payload = serialize_run_record(
            run_record,
            equity_curve=result.equity_curve,
            fills=result.fills,
        )
        artifact_path = self.experiment_store.save_run(run_record, payload)
        payload["artifact_path"] = artifact_path
        self.experiment_store.save_run(run_record, payload)

        return BacktestRunResult(
            run_id=run_id,
            metrics=result.metrics,
            artifact_path=artifact_path,
        )

    def get_run(self, run_id: str) -> dict[str, Any]:
        record = self.experiment_store.get_run(run_id)
        if record is None:
            raise DataNotFoundError(f"run_id not found: {run_id}")
        return record

