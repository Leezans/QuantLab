from __future__ import annotations

import streamlit as st

from ui.components.charts import render_dataframe_preview, render_factor_line_chart
from ui.components.inputs import render_symbol_input, render_time_range_inputs
from ui.components.status import render_errors
from ui.services.contracts import LabService
from ui.services.types.common import EnsureFactorsRequest


def render_factors_view(
    service: LabService,
    *,
    panel_key: str,
    default_symbol: str,
    default_market: str,
) -> None:
    st.subheader("Factor Explorer")

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
        factor_set = st.selectbox(
            "Factor Set",
            options=["basic"],
            index=0,
            key=f"{panel_key}_{service.lab_key()}_factor_set",
        )

    run = st.button("Load / Compute Factors", type="primary", key=f"{panel_key}_{service.lab_key()}_run_factors")
    if not run:
        return

    req = EnsureFactorsRequest(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        market=market,
        style=style,
        factor_set=factor_set,
    )
    with st.spinner("Computing factors..."):
        result = service.ensure_factors(req)

    st.caption(f"Source: {result.source} | Input source: {result.input_source}")
    if result.cache_path:
        st.caption(f"Cache path: {result.cache_path}")

    render_errors(result.errors)

    st.subheader("Factor Preview")
    render_dataframe_preview(result.dataframe)
    st.subheader("Factor Chart")
    render_factor_line_chart(result.dataframe)
