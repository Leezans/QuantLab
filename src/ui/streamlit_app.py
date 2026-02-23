from __future__ import annotations

import streamlit as st

from ui.pages.data_explorer import render_data_explorer
from ui.pages.pipelines import render_pipelines
from ui.pages.trades_downloader import render_trades_downloader
from ui.services.registry import get_lab_service, list_labs


st.set_page_config(page_title="QuantLab", layout="wide")
st.title("QuantLab UI")

labs = list_labs()
default_lab_idx = 0 if labs else None

with st.sidebar:
    st.header("Navigation")
    lab = st.selectbox("Lab", options=labs, index=default_lab_idx) if labs else None
    page = st.radio(
        "Page",
        options=["Data Explorer", "Pipelines", "Trades Downloader"],
        index=0,
    )

if lab is None:
    st.error("No lab service available. Please register at least one lab in ui/services/registry.py")
else:
    service = get_lab_service(lab)

    if page == "Data Explorer":
        render_data_explorer(service)
    elif page == "Pipelines":
        render_pipelines(service)
    else:
        render_trades_downloader()
