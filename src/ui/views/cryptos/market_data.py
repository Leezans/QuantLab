from __future__ import annotations

import streamlit as st

from ui.components.charts_lightweight import (
    render_klines_chart,
    render_trades_price_chart,
    render_volume_profile_chart,
)
from ui.components.inputs import (
    render_date_range_inputs,
    render_market_layout_inputs,
    render_symbol_input,
    render_time_range_inputs,
)
from ui.components.status import render_download_summary, render_errors, render_paths
from ui.components.views import render_preview_table
from ui.services.orchestrators.market_data import (
    compute_volume_profile_from_trades,
    get_or_create_klines,
    get_or_create_trades,
)
from ui.services.types.cryptos import (
    KlinesRequestDTO,
    KlinesResultDTO,
    TradesRequestDTO,
    TradesResultDTO,
)

_MAX_CHART_POINTS = 5000


def render_market_data_tab() -> None:
    """Render CryptoLab Data tab with nested Klines/Trades views."""
    tabs = st.tabs(["Klines", "Trades"])
    with tabs[0]:
        _render_klines_panel()
    with tabs[1]:
        _render_trades_panel()


def _render_klines_panel() -> None:
    st.subheader("Klines")
    symbol = render_symbol_input(key_prefix="crypto_data_klines", symbols=[], default_symbol="BTCUSDT")
    start, end, interval = render_time_range_inputs(
        key_prefix="crypto_data_klines",
        default_start="2024-01-01",
        default_end="2024-01-07",
        interval_options=["1m", "5m", "1h", "1d"],
        default_interval="1h",
    )
    market, layout = render_market_layout_inputs(
        key_prefix="crypto_data_klines",
        default_market="spot",
        default_layout="mirror",
    )

    run = st.button("Load Klines", type="primary", key="crypto_data_klines_run")
    if run:
        req = KlinesRequestDTO(
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            market=market,
            style=layout,
        )
        with st.spinner("Loading klines (cache-first)..."):
            st.session_state["crypto_data_klines_result"] = get_or_create_klines(req)

    result = st.session_state.get("crypto_data_klines_result")
    if result is None:
        st.info("Submit the form to load klines.")
        return

    _render_klines_result(result)

    st.markdown("---")
    _render_klines_volume_profile_controls(symbol=symbol, start=start, end=end, market=market, layout=layout)


def _render_klines_result(result: KlinesResultDTO) -> None:
    render_download_summary(
        source=result.source,
        ok=result.ok,
        skipped=result.skipped,
        failed=result.failed,
        row_count=result.row_count,
    )
    render_paths(result.parquet_paths, title="Klines Parquet Paths")
    render_errors(result.errors)
    render_preview_table(result.preview, rows=300, title="Klines Preview")

    if result.preview.empty:
        st.info("No klines rows available for chart.")
        return

    plotted_rows, limited = render_klines_chart(
        result.preview.reset_index(),
        key="crypto_data_klines_chart",
        max_points=_MAX_CHART_POINTS,
    )
    if limited:
        st.warning(f"Kline points exceed {_MAX_CHART_POINTS}. Downsampled for rendering.")
    st.caption(f"Plotted rows: {plotted_rows}")


def _render_klines_volume_profile_controls(
    *,
    symbol: str,
    start: str,
    end: str,
    market: str,
    layout: str,
) -> None:
    st.subheader("Volume Profile (from Trades)")
    c1, c2, c3 = st.columns(3)
    with c1:
        bins = st.number_input("Bins", min_value=10, max_value=300, value=80, step=5, key="crypto_vp_bins_kline")
    with c2:
        volume_type = st.selectbox(
            "Volume Type",
            options=["base", "quote"],
            index=0,
            key="crypto_vp_type_kline",
        )
    with c3:
        normalize = st.checkbox("Normalize", value=False, key="crypto_vp_norm_kline")

    run_profile = st.button("Build Volume Profile", key="crypto_vp_run_k")
    if not run_profile:
        return

    trades_req = TradesRequestDTO(
        symbol=symbol,
        start=start,
        end=end,
        market=market,
        style=layout,
        preview_rows=5000,
    )
    with st.spinner("Loading trades preview for volume profile..."):
        trades_result = get_or_create_trades(trades_req)

    profile = compute_volume_profile_from_trades(
        trades_result.preview.reset_index(),
        bins=int(bins),
        volume_type=volume_type,
        normalize=normalize,
    )
    render_volume_profile_chart(profile, title="Klines Range Volume Profile")


def _render_trades_panel() -> None:
    st.subheader("Trades")
    symbol = render_symbol_input(key_prefix="crypto_data_trades", symbols=[], default_symbol="BTCUSDT")
    start, end = render_date_range_inputs(
        key_prefix="crypto_data_trades",
        default_start="2024-01-01",
        default_end="2024-01-07",
    )
    market, layout = render_market_layout_inputs(
        key_prefix="crypto_data_trades",
        default_market="spot",
        default_layout="mirror",
    )
    run = st.button("Load Trades", type="primary", key="crypto_data_trades_run")
    if run:
        req = TradesRequestDTO(
            symbol=symbol,
            start=start,
            end=end,
            market=market,
            style=layout,
        )
        with st.spinner("Loading trades (cache-first)..."):
            st.session_state["crypto_data_trades_result"] = get_or_create_trades(req)

    result = st.session_state.get("crypto_data_trades_result")
    if result is None:
        st.info("Submit the form to load trades.")
        return

    _render_trades_result(result)
    st.markdown("---")
    _render_trades_volume_profile(result)


def _render_trades_result(result: TradesResultDTO) -> None:
    render_download_summary(
        source=result.source,
        ok=result.ok,
        skipped=result.skipped,
        failed=result.failed,
        row_count=result.row_count,
    )
    render_paths(result.parquet_paths, title="Trades Parquet Paths")
    render_errors(result.errors)
    render_preview_table(result.preview, rows=300, title="Trades Preview")

    if result.preview.empty:
        st.info("No trades rows available for chart.")
        return

    plotted_rows, limited = render_trades_price_chart(
        result.preview.reset_index(),
        key="crypto_data_trades_chart",
        max_points=_MAX_CHART_POINTS,
    )
    if limited:
        st.warning(f"Trade points exceed {_MAX_CHART_POINTS}. Downsampled for rendering.")
    st.caption(f"Plotted rows: {plotted_rows}")


def _render_trades_volume_profile(result: TradesResultDTO) -> None:
    st.subheader("Volume Profile")
    c1, c2, c3 = st.columns(3)
    with c1:
        bins = st.number_input("Bins", min_value=10, max_value=300, value=80, step=5, key="crypto_vp_bins_trade")
    with c2:
        volume_type = st.selectbox(
            "Volume Type",
            options=["base", "quote"],
            index=0,
            key="crypto_vp_type_trade",
        )
    with c3:
        normalize = st.checkbox("Normalize", value=False, key="crypto_vp_norm_trade")

    profile = compute_volume_profile_from_trades(
        result.preview.reset_index(),
        bins=int(bins),
        volume_type=volume_type,
        normalize=normalize,
    )
    render_volume_profile_chart(profile, title="Trades Volume Profile")
