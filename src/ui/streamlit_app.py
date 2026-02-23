# src/ui/streamlit_app.py
from __future__ import annotations

import streamlit as st

from ui.services.registry import list_labs

st.set_page_config(page_title="QuantLab", layout="wide")
st.title("QuantLab")
st.markdown("---")

labs = list_labs()
if not labs:
    st.error("No lab service available. Please register at least one lab in ui/services/registry.py")
else:
    st.info(" Welcome to QuantLab! Select a page from the sidebar to explore data, run pipelines, or download trades.")
    st.markdown("""
## Available Pages:
- ** Data Explorer** - Explore historical time series data
- ** Pipelines** - Run data processing pipelines
- ** Trades Downloader** - Download trades from Binance
    """)
