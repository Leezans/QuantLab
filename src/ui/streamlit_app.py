# src/ui/streamlit_app.py
from __future__ import annotations

import streamlit as st
from ui.services.registry import get_service_mode, list_labs

st.set_page_config(page_title="QuantLab", layout="wide")
st.title("QuantLab")
st.markdown("---")

labs = list_labs()
if not labs:
    st.error("No lab service available. Please register at least one lab in ui/services/registry.py")
else:
    st.info("Select a lab page from the sidebar.")
    st.caption(f"Service mode: {get_service_mode()}")
    st.write("Available labs:", ", ".join(labs))
    st.markdown("""
## Architecture Contract
- pages only orchestrate views
- views only call services
- cLab imports are isolated in services/direct
    """)
