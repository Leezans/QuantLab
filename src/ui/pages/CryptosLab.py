from __future__ import annotations

import streamlit as st

from ui.services.registry import get_lab_service
from ui.views.cryptos.factors import render_factors
from ui.views.cryptos.kline import render_kline
from ui.views.cryptos.overview import render_overview
from ui.views.cryptos.trades import render_trades

st.set_page_config(page_title="CryptosLab", layout="wide")

service = get_lab_service("crypto")

st.title("cLab / CryptosLab")
st.caption("Page only orchestrates views. Views only call services.")

tabs = st.tabs(["Overview", "Kline", "Trades", "Factors"])

with tabs[0]:
    render_overview(service)

with tabs[1]:
    render_kline(service)

with tabs[2]:
    render_trades(service)

with tabs[3]:
    render_factors(service)
