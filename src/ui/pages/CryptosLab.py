from __future__ import annotations

import streamlit as st

from ui.views.cryptos.market_data import render_market_data_tab

st.set_page_config(page_title="CryptosLab", layout="wide")

st.title("cLab / CryptosLab")
st.caption("Page only orchestrates views. Views only call services.")

tabs = st.tabs(["Data", "Factors"])

with tabs[0]:
    render_market_data_tab()

with tabs[1]:
    st.info("Factors tab is reserved for the next iteration.")
