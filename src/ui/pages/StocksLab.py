from __future__ import annotations

import streamlit as st

from ui.services.registry import get_lab_service
from ui.views.stocks.factors import render_factors
from ui.views.stocks.kline import render_kline
from ui.views.stocks.overview import render_overview
from ui.views.stocks.universe import render_universe

st.set_page_config(page_title="StocksLab", layout="wide")

service = get_lab_service("stocks")

st.title("sLab / StocksLab")
st.caption("Page only orchestrates views. Views only call services.")

tabs = st.tabs(["Overview", "Universe", "Kline", "Factors"])

with tabs[0]:
    render_overview(service)

with tabs[1]:
    render_universe(service)

with tabs[2]:
    render_kline(service)

with tabs[3]:
    render_factors(service)
