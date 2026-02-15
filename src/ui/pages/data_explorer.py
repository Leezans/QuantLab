from __future__ import annotations

import streamlit as st

from ui.components.views import render_timeseries_view
from ui.services.types import LabService


def render_data_explorer(service: LabService) -> None:
    st.subheader("Data Explorer")

    datasets = service.list_datasets()
    if not datasets:
        st.info("No datasets found under CLAB_FILE_DB_ROOT.")
        return

    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        dataset = st.selectbox("Dataset", options=datasets, index=0)
    with c2:
        symbols = service.list_symbols(dataset)
        symbol = st.selectbox("Symbol", options=symbols, index=0) if symbols else ""
    with c3:
        dates = service.list_dates(dataset, symbol) if symbol else []
        date_str = st.selectbox("Date", options=dates, index=len(dates) - 1) if dates else ""

    if not symbol or not date_str:
        st.info("Pick a dataset/symbol/date.")
        return

    with st.expander("Manifest", expanded=True):
        m = service.load_manifest(dataset, symbol, date_str)
        st.json(m or {"note": "manifest.json not found"})

    st.divider()
    st.caption("Preview")
    limit = st.slider("Preview rows", min_value=20, max_value=500, value=200, step=20)
    preview = service.load_preview(dataset, symbol, date_str, limit=int(limit))

    kind = preview.get("kind")
    if kind == "parquet":
        import pandas as pd

        df = pd.DataFrame(preview.get("rows", []))
        # Try to render a timeseries chart if minute exists.
        if "minute" in df.columns:
            df["minute"] = pd.to_datetime(df["minute"], utc=True, errors="coerce")
            df = df.dropna(subset=["minute"]).sort_values("minute").set_index("minute")
        render_timeseries_view(df)
    elif kind == "json":
        st.json(preview.get("data"))
    elif kind == "jsonl":
        st.write(preview.get("rows", [])[: min(50, len(preview.get("rows", [])))])
        st.caption("(showing up to 50 lines)")
    else:
        st.warning("No previewable files found for this partition.")
