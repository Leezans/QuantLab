from __future__ import annotations

import streamlit as st

from ui.services.contracts import LabService


def render_overview(service: LabService) -> None:
    st.subheader("Overview")
    st.write("sLab UI layer is ready. Backend wiring to src/sLab will be added later.")
    st.caption(f"Service: {service.display_name()} | Key: {service.lab_key()}")
