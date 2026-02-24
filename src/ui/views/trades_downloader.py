from __future__ import annotations

import streamlit as st

from ui.components.inputs import render_symbol_input, render_time_range_inputs
from ui.components.status import render_errors, render_paths
from ui.services.contracts import LabService
from ui.services.types.cryptos import TradesRangeRequest


def render_trades_downloader(service: LabService, *, panel_key: str, default_symbol: str, default_market: str) -> None:
    st.subheader("Trades Downloader")
    if not service.supports_trades_download():
        st.info(f"{service.display_name()} does not support trades downloader yet.")
        return

    symbol = render_symbol_input(
        key_prefix=f"{panel_key}_{service.lab_key()}",
        symbols=service.list_symbols(),
        default_symbol=default_symbol,
    )
    start, end, _ = render_time_range_inputs(
        key_prefix=f"{panel_key}_{service.lab_key()}",
        default_start="2024-01-01",
        default_end="2024-01-07",
        interval_options=["1d"],
        default_interval="1d",
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        market = st.selectbox(
            "Market",
            options=["spot", "futures"],
            index=0 if default_market == "spot" else 1,
            key=f"{panel_key}_{service.lab_key()}_market",
        )
    with c2:
        style = st.selectbox(
            "Layout",
            options=["mirror", "hive"],
            index=0,
            key=f"{panel_key}_{service.lab_key()}_style",
        )
    with c3:
        verify = st.checkbox("Verify checksum", value=True, key=f"{panel_key}_{service.lab_key()}_verify")

    run = st.button("Run Trades Pipeline", type="primary", key=f"{panel_key}_{service.lab_key()}_run_trades")
    if not run:
        return

    req = TradesRangeRequest(
        symbol=symbol,
        start=start,
        end=end,
        market=market,
        style=style,
        verify_checksum=verify,
    )

    with st.spinner("Running trades pipeline..."):
        result = service.run_trades_range(req)

    st.caption(f"Source: {result.source}")
    a, b, c, d = st.columns(4)
    a.metric("Total days", result.total_days)
    b.metric("OK", result.ok)
    c.metric("Skipped", result.skipped)
    d.metric("Failed", result.failed)

    render_paths(result.parquet_paths)
    render_errors(result.errors)
