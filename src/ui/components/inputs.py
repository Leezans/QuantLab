from __future__ import annotations

import streamlit as st


def render_symbol_input(key_prefix: str, symbols: list[str], default_symbol: str) -> str:
    default_symbol = default_symbol.strip().upper()
    if symbols:
        options = [s.strip().upper() for s in symbols]
        index = options.index(default_symbol) if default_symbol in options else 0
        return st.selectbox("Symbol", options=options, index=index, key=f"{key_prefix}_symbol")
    return st.text_input("Symbol", value=default_symbol, key=f"{key_prefix}_symbol")


def render_time_range_inputs(
    key_prefix: str,
    *,
    default_start: str = "2024-01-01",
    default_end: str = "2024-01-07",
    interval_options: list[str] | None = None,
    default_interval: str = "1m",
) -> tuple[str, str, str]:
    options = interval_options or ["1m", "5m", "1h", "1d"]
    default_index = options.index(default_interval) if default_interval in options else 0

    c1, c2, c3 = st.columns(3)
    with c1:
        start = st.text_input("Start (YYYY-MM-DD)", value=default_start, key=f"{key_prefix}_start")
    with c2:
        end = st.text_input("End (YYYY-MM-DD)", value=default_end, key=f"{key_prefix}_end")
    with c3:
        interval = st.selectbox("Interval", options=options, index=default_index, key=f"{key_prefix}_interval")

    return start, end, interval
