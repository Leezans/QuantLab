from __future__ import annotations

import streamlit as st

from ui.components.inputs import render_time_range_inputs
from ui.components.views import render_timeseries_view
from ui.services.types import LabService


def render_data_explorer(service: LabService) -> None:
    st.subheader("Data Explorer")

    symbols = service.list_symbols()
    if not symbols:
        st.info("No symbols available.")
        return

    c1, c2 = st.columns([2, 3])
    with c1:
        symbol = st.selectbox("Symbol", options=symbols, index=0)
        start, end, freq = render_time_range_inputs()

    with st.spinner("Loading data..."):
        df = service.load_timeseries(symbol=symbol, start=start, end=end, freq=freq)

    render_timeseries_view(df)
