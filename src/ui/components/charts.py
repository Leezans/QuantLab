from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


def render_dataframe_preview(df: pd.DataFrame, rows: int = 200) -> None:
    st.dataframe(df.head(rows), use_container_width=True)


def _time_axis(frame: pd.DataFrame) -> pd.Series:
    if "timestamp" in frame.columns:
        return pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    if pd.api.types.is_datetime64_any_dtype(frame.index):
        return pd.to_datetime(frame.index, utc=True, errors="coerce")
    return pd.to_datetime(frame.index, utc=True, errors="coerce")


def _limit_rows(frame: pd.DataFrame, max_points: int) -> tuple[pd.DataFrame, bool]:
    if max_points <= 0 or frame.shape[0] <= max_points:
        return frame, False

    # 点位过多时按步长抽样，避免前端渲染卡顿。
    step = max(1, int(np.ceil(frame.shape[0] / max_points)))
    limited = frame.iloc[::step].copy()
    return limited, True


def build_candlestick_figure(
    frame: pd.DataFrame,
    *,
    max_points: int = 5000,
    include_volume: bool = True,
) -> tuple[go.Figure, pd.DataFrame, bool]:
    working = frame.copy()
    if "timestamp" not in working.columns and pd.api.types.is_datetime64_any_dtype(working.index):
        working = working.reset_index()
        if "index" in working.columns and "timestamp" not in working.columns:
            working = working.rename(columns={"index": "timestamp"})

    for col in ("open", "high", "low", "close", "volume"):
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce")

    working["timestamp"] = pd.to_datetime(working.get("timestamp", _time_axis(working)), utc=True, errors="coerce")
    working = working.dropna(subset=["timestamp", "open", "high", "low", "close"]) if not working.empty else working
    working = working.sort_values("timestamp")

    plot_df, limited = _limit_rows(working, max_points=max_points)

    include_volume = include_volume and ("volume" in plot_df.columns)
    if include_volume:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.75, 0.25],
        )
        fig.add_trace(
            go.Candlestick(
                x=plot_df["timestamp"],
                open=plot_df["open"],
                high=plot_df["high"],
                low=plot_df["low"],
                close=plot_df["close"],
                name="Kline",
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=plot_df["timestamp"],
                y=plot_df["volume"],
                name="Volume",
                marker_color="#7FA6C6",
            ),
            row=2,
            col=1,
        )
    else:
        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=plot_df["timestamp"],
                    open=plot_df["open"],
                    high=plot_df["high"],
                    low=plot_df["low"],
                    close=plot_df["close"],
                    name="Kline",
                )
            ],
        )

    fig.update_layout(
        title="Klines",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", y=1.02, x=0),
        margin=dict(l=10, r=10, t=40, b=20),
    )

    return fig, plot_df, limited


def build_trades_price_figure(
    frame: pd.DataFrame,
    *,
    max_points: int = 5000,
) -> tuple[go.Figure, pd.DataFrame, bool]:
    working = frame.copy()
    if "timestamp" not in working.columns and pd.api.types.is_datetime64_any_dtype(working.index):
        working = working.reset_index()
        if "index" in working.columns and "timestamp" not in working.columns:
            working = working.rename(columns={"index": "timestamp"})

    working["timestamp"] = pd.to_datetime(working.get("timestamp", _time_axis(working)), utc=True, errors="coerce")
    working["price"] = pd.to_numeric(working.get("price"), errors="coerce")
    working = working.dropna(subset=["timestamp", "price"]) if not working.empty else working
    working = working.sort_values("timestamp")

    plot_df, limited = _limit_rows(working, max_points=max_points)

    fig = go.Figure()
    fig.add_trace(
        go.Scattergl(
            x=plot_df["timestamp"],
            y=plot_df["price"],
            mode="lines+markers",
            marker=dict(size=3),
            line=dict(width=1),
            name="Trade Price",
        )
    )
    fig.update_layout(
        title="Trades Price",
        margin=dict(l=10, r=10, t=40, b=20),
        xaxis_title="Time",
        yaxis_title="Price",
    )
    return fig, plot_df, limited


def build_volume_profile_figure(
    trades: pd.DataFrame,
    *,
    bins: int = 80,
    volume_type: str = "base",
    normalize: bool = False,
) -> go.Figure | None:
    if trades.empty or "price" not in trades.columns:
        return None

    prices = pd.to_numeric(trades["price"], errors="coerce")
    mask = prices.notna()
    prices = prices.loc[mask]
    if prices.empty:
        return None

    if volume_type == "quote" and "quote_quantity" in trades.columns:
        weights = pd.to_numeric(trades.loc[mask, "quote_quantity"], errors="coerce").fillna(0.0)
    elif "quantity" in trades.columns:
        weights = pd.to_numeric(trades.loc[mask, "quantity"], errors="coerce").fillna(0.0)
    else:
        weights = pd.Series(np.ones(len(prices)), index=prices.index)

    # 基于价格分箱统计成交量分布（筹码分布近似）。
    hist, edges = np.histogram(prices.to_numpy(), bins=max(1, bins), weights=weights.to_numpy())
    centers = (edges[:-1] + edges[1:]) / 2.0

    values = hist.astype(float)
    if normalize:
        denom = values.sum()
        if denom > 0:
            values = values / denom * 100.0

    fig = go.Figure(
        data=[
            go.Bar(
                x=values,
                y=centers,
                orientation="h",
                name="Volume Profile",
            )
        ],
    )
    x_title = "Volume %" if normalize else "Volume"
    fig.update_layout(
        title="Volume Profile",
        xaxis_title=x_title,
        yaxis_title="Price",
        margin=dict(l=10, r=10, t=40, b=20),
    )
    return fig


def render_close_line_chart(df: pd.DataFrame) -> None:
    if "close" not in df.columns:
        st.info("Column 'close' not found, skip line chart.")
        return
    st.line_chart(df["close"])


def render_factor_line_chart(df: pd.DataFrame) -> None:
    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        st.info("No numeric factor columns found.")
        return
    st.line_chart(numeric.tail(300))
