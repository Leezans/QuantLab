from __future__ import annotations

import streamlit as st

from ui.services.contracts import LabService


def render_overview(service: LabService) -> None:
    st.subheader("Overview")
    st.write("cLab is the current active backend. Data loading follows cache-first behavior.")
    st.caption(f"Service: {service.display_name()} | Key: {service.lab_key()}")
