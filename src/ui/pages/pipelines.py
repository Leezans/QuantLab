# ui/pages/pipelines.py
from __future__ import annotations

import streamlit as st

from ui.services.registry import get_lab_service, list_labs

st.set_page_config(page_title="Pipelines", layout="wide")
st.subheader("⚙️ Pipelines")

labs = list_labs()
default_lab_idx = 0 if labs else None

with st.sidebar:
    st.header("Configuration")
    lab = st.selectbox("Lab", options=labs, index=default_lab_idx) if labs else None

if lab is None:
    st.error("No lab service available.")
else:
    service = get_lab_service(lab)
    
    c1, c2 = st.columns([2, 3])
    with c1:
        symbols = service.list_symbols()
        symbol = st.selectbox("Symbol", options=symbols, index=0) if symbols else ""
        run = st.button("Run: Build Features (stub)")

    with c2:
        st.caption("This page triggers application-layer use cases via lab app/api.")

    if run:
        with st.spinner("Running pipeline..."):
            result = service.run_pipeline_build_features(symbol=symbol)
        st.success("Done.")
        st.json(result)
