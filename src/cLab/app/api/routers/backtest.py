from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from cLab.app.api.deps import get_backtest_pipeline
from cLab.app.dto import (
    BacktestRecordResponseDTO,
    BacktestRunRequestDTO,
    BacktestRunResponseDTO,
    BacktestSummaryDTO,
)
from cLab.pipelines import BacktestPipeline, BacktestRunCommand
from cLab.core.domain.errors import BacktestError, DataNotFoundError, ValidationError
from cLab.core.domain.types import StrategyParams

router = APIRouter(prefix="/api/backtest", tags=["backtest"])


@router.post("/run", response_model=BacktestRunResponseDTO)
def run_backtest(
    request: BacktestRunRequestDTO,
    pipeline: Annotated[BacktestPipeline, Depends(get_backtest_pipeline)],
) -> BacktestRunResponseDTO:
    try:
        command = BacktestRunCommand(
            symbol=request.symbol,
            start=request.start,
            end=request.end,
            interval=request.interval,
            market=request.market,
            style=request.style,
            strategy_params=StrategyParams(
                fast_window=request.fast_window,
                slow_window=request.slow_window,
            ),
            fee_bps=request.fee_bps,
            slippage_bps=request.slippage_bps,
            initial_cash=request.initial_cash,
            seed=request.seed,
        )
        result = pipeline.run(command)
        summary = BacktestSummaryDTO(
            total_return=result.metrics.total_return,
            max_drawdown=result.metrics.max_drawdown,
            sharpe_ratio=result.metrics.sharpe_ratio,
            final_equity=result.metrics.final_equity,
            trade_count=result.metrics.trade_count,
        )
        return BacktestRunResponseDTO(
            run_id=result.run_id,
            artifact_path=result.artifact_path,
            summary=summary,
        )
    except (ValidationError, BacktestError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DataNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc


@router.get("/{run_id}", response_model=BacktestRecordResponseDTO)
def get_backtest(
    run_id: str,
    pipeline: Annotated[BacktestPipeline, Depends(get_backtest_pipeline)],
) -> BacktestRecordResponseDTO:
    try:
        record = pipeline.get_run(run_id)
        return BacktestRecordResponseDTO(run=record)
    except DataNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc
