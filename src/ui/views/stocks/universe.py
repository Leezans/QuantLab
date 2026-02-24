from __future__ import annotations

import streamlit as st

from ui.services.contracts import LabService


def render_universe(service: LabService) -> None:
    st.subheader("Universe")
    symbols = service.list_symbols()
    if not symbols:
        st.info("No symbols available.")
        return

    query = st.text_input("Filter symbol", value="", key="stocks_universe_filter").strip().upper()
    shown = [s for s in symbols if query in s.upper()] if query else symbols
    st.write(f"Total: {len(shown)}")
    st.dataframe({"symbol": shown}, use_container_width=True)
