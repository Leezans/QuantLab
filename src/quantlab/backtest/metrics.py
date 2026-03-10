from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import fmean, pstdev
from typing import Sequence


@dataclass(frozen=True, slots=True)
class PerformanceMetrics:
    pnl: float
    total_return: float
    sharpe: float
    max_drawdown: float
    turnover: float
    annualized_volatility: float


def compute_metrics(equity_curve: Sequence[float], turnover: float) -> PerformanceMetrics:
    if not equity_curve:
        return PerformanceMetrics(
            pnl=0.0,
            total_return=0.0,
            sharpe=0.0,
            max_drawdown=0.0,
            turnover=turnover,
            annualized_volatility=0.0,
        )

    pnl = equity_curve[-1] - equity_curve[0]
    total_return = 0.0 if equity_curve[0] == 0 else (equity_curve[-1] / equity_curve[0]) - 1.0
    returns = [
        (equity_curve[index] / equity_curve[index - 1]) - 1.0
        for index in range(1, len(equity_curve))
        if equity_curve[index - 1] != 0
    ]
    if len(returns) > 1 and pstdev(returns) != 0:
        sharpe = sqrt(252.0) * fmean(returns) / pstdev(returns)
        annualized_volatility = pstdev(returns) * sqrt(252.0)
    else:
        sharpe = 0.0
        annualized_volatility = 0.0

    peak = equity_curve[0]
    max_drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak != 0:
            max_drawdown = max(max_drawdown, (peak - value) / peak)

    return PerformanceMetrics(
        pnl=pnl,
        total_return=total_return,
        sharpe=sharpe,
        max_drawdown=max_drawdown,
        turnover=turnover,
        annualized_volatility=annualized_volatility,
    )

