from __future__ import annotations

import streamlit as st


def render_time_range_inputs() -> tuple[str, str, str]:
    start = st.text_input("Start", value="2024-01-01")
    end = st.text_input("End", value="2024-01-02")
    freq = st.selectbox("Freq", options=["1m", "5m", "1h", "1d"], index=0)
    return start, end, freq
