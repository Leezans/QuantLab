from __future__ import annotations

import streamlit as st

from ui.services.types import LabService


def render_pipelines(service: LabService) -> None:
    st.subheader("Pipelines")

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
