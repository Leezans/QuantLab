from __future__ import annotations

import numpy as np
import pandas as pd

from cLab.core.domain.errors import BacktestError
from cLab.core.domain.types import BacktestMetrics, BacktestResult, Fill, StrategyParams


def run_ma_crossover_backtest(
    *,
    bars: pd.DataFrame,
    strategy_params: StrategyParams,
    fee_bps: float,
    slippage_bps: float,
    initial_cash: float,
) -> BacktestResult:
    closes = pd.to_numeric(bars["close"], errors="coerce")
    timestamps = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
    valid = closes.notna() & timestamps.notna()
    closes = closes.loc[valid].reset_index(drop=True)
    timestamps = timestamps.loc[valid].reset_index(drop=True)

    if len(closes) < strategy_params.slow_window + 1:
        raise BacktestError(
            f"Not enough bars for strategy windows: got={len(closes)}, need>{strategy_params.slow_window}",
        )

    fast = closes.rolling(window=strategy_params.fast_window, min_periods=strategy_params.fast_window).mean()
    slow = closes.rolling(window=strategy_params.slow_window, min_periods=strategy_params.slow_window).mean()
    signal = (fast > slow).astype(int)
    signal_prev = signal.shift(1).fillna(0).astype(int)

    entry = (signal == 1) & (signal_prev == 0)
    exit_ = (signal == 0) & (signal_prev == 1)

    fee_rate = float(fee_bps) / 10_000.0
    slippage_rate = float(slippage_bps) / 10_000.0

    cash = float(initial_cash)
    position_qty = 0.0
    equity_curve: list[float] = []
    fills: list[Fill] = []

    for i in range(len(closes)):
        px = float(closes.iloc[i])
        ts = timestamps.iloc[i].to_pydatetime()

        if bool(entry.iloc[i]) and position_qty <= 0.0 and cash > 0.0:
            buy_px = px * (1.0 + slippage_rate)
            quantity = cash / (buy_px * (1.0 + fee_rate))
            notional = quantity * buy_px
            fee = notional * fee_rate
            cash -= notional + fee
            if cash < 0.0 and abs(cash) < 1e-8:
                cash = 0.0
            position_qty = quantity
            fills.append(
                Fill(
                    timestamp=ts,
                    side="buy",
                    quantity=quantity,
                    price=buy_px,
                    fee=fee,
                ),
            )

        if bool(exit_.iloc[i]) and position_qty > 0.0:
            sell_px = px * (1.0 - slippage_rate)
            gross = position_qty * sell_px
            fee = gross * fee_rate
            cash += gross - fee
            fills.append(
                Fill(
                    timestamp=ts,
                    side="sell",
                    quantity=position_qty,
                    price=sell_px,
                    fee=fee,
                ),
            )
            position_qty = 0.0

        equity_curve.append(cash + position_qty * px)

    if not equity_curve:
        raise BacktestError("Backtest produced an empty equity curve")

    metrics = _compute_metrics(
        equity_curve=equity_curve,
        initial_cash=initial_cash,
        trade_count=len(fills),
    )
    return BacktestResult(metrics=metrics, equity_curve=equity_curve, fills=fills)


def _compute_metrics(*, equity_curve: list[float], initial_cash: float, trade_count: int) -> BacktestMetrics:
    equity = np.asarray(equity_curve, dtype=float)
    final_equity = float(equity[-1])
    total_return = (final_equity / float(initial_cash)) - 1.0

    peaks = np.maximum.accumulate(equity)
    drawdowns = np.where(peaks > 0.0, equity / peaks - 1.0, 0.0)
    max_drawdown = float(np.min(drawdowns)) if drawdowns.size else 0.0

    returns = pd.Series(equity, dtype=float).pct_change().dropna()
    if returns.empty or float(returns.std(ddof=0)) == 0.0:
        sharpe = 0.0
    else:
        sharpe = float((returns.mean() / returns.std(ddof=0)) * np.sqrt(252.0))

    return BacktestMetrics(
        total_return=total_return,
        max_drawdown=max_drawdown,
        sharpe_ratio=sharpe,
        final_equity=final_equity,
        trade_count=trade_count,
    )

