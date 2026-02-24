from __future__ import annotations

import streamlit as st

from ui.components.charts import render_close_line_chart, render_dataframe_preview
from ui.components.inputs import render_symbol_input, render_time_range_inputs
from ui.components.status import render_data_status, render_errors, render_paths
from ui.services.contracts import LabService
from ui.services.types.common import EnsureKlinesRequest


def render_market_data_view(
    service: LabService,
    *,
    panel_key: str,
    default_symbol: str,
    default_market: str,
) -> None:
    st.subheader("Kline Explorer")

    symbol = render_symbol_input(
        key_prefix=f"{panel_key}_{service.lab_key()}",
        symbols=service.list_symbols(),
        default_symbol=default_symbol,
    )
    start, end, interval = render_time_range_inputs(
        key_prefix=f"{panel_key}_{service.lab_key()}",
        default_start="2024-01-01",
        default_end="2024-01-07",
        interval_options=["1m", "5m", "1h", "1d"],
        default_interval="1m",
    )

    market = default_market
    style = "mirror"
    if service.lab_key() in {"crypto", "futures"}:
        c1, c2 = st.columns(2)
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

    run = st.button("Load / Fetch Klines", type="primary", key=f"{panel_key}_{service.lab_key()}_run_klines")
    if not run:
        return

    req = EnsureKlinesRequest(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        market=market,
        style=style,
    )

    with st.spinner("Loading data..."):
        result = service.ensure_klines(req)

    render_data_status(
        source=result.source,
        total=result.total_days,
        cached=result.cached_days,
        fetched=result.fetched_days,
        failed=result.failed_days,
    )
    render_errors(result.errors)
    render_paths(result.parquet_paths)

    st.subheader("Data Preview")
    render_dataframe_preview(result.dataframe)
    st.subheader("Close Chart")
    render_close_line_chart(result.dataframe)
