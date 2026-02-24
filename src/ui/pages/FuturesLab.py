from __future__ import annotations

import streamlit as st

from ui.services.registry import get_lab_service
from ui.views.futures.factors import render_factors
from ui.views.futures.kline import render_kline
from ui.views.futures.overview import render_overview

st.set_page_config(page_title="FuturesLab", layout="wide")

service = get_lab_service("futures")

st.title("fLab / FuturesLab")
st.caption("Page only orchestrates views. Views only call services.")

tabs = st.tabs(["Overview", "Kline", "Factors"])

with tabs[0]:
    render_overview(service)

with tabs[1]:
    render_kline(service)

with tabs[2]:
    render_factors(service)
