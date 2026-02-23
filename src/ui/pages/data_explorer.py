# ui/pages/data_explorer.py
from __future__ import annotations

import streamlit as st

from ui.components.inputs import render_time_range_inputs
from ui.components.views import render_timeseries_view
from ui.services.registry import get_lab_service, list_labs

st.set_page_config(page_title="Data Explorer", layout="wide")
st.subheader(" Data Explorer")

labs = list_labs()
default_lab_idx = 0 if labs else None

with st.sidebar:
    st.header("Configuration")
    lab = st.selectbox("Lab", options=labs, index=default_lab_idx) if labs else None

if lab is None:
    st.error("No lab service available.")
else:
    service = get_lab_service(lab)
    
    symbols = service.list_symbols()
    if not symbols:
        st.info("No symbols available.")
    else:
        c1, c2 = st.columns([2, 3])
        with c1:
            symbol = st.selectbox("Symbol", options=symbols, index=0)
            start, end, freq = render_time_range_inputs()

        with st.spinner("Loading data..."):
            df = service.load_timeseries(symbol=symbol, start=start, end=end, freq=freq)

        render_timeseries_view(df)
