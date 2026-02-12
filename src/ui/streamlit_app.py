from __future__ import annotations

import streamlit as st

from ui.pages.data_explorer import render_data_explorer
from ui.pages.pipelines import render_pipelines
from ui.services.registry import list_labs, get_lab_service


def main() -> None:
    st.set_page_config(page_title="QuantLab UI", layout="wide")
    st.title("QuantLab UI")

    labs = list_labs()
    if not labs:
        st.error("No labs registered.")
        return

    with st.sidebar:
        st.header("Navigation")
        lab_key = st.selectbox("Lab", options=labs, index=0)
        page = st.selectbox("Page", options=["Data Explorer", "Pipelines"], index=0)

    service = get_lab_service(lab_key)

    if page == "Data Explorer":
        render_data_explorer(service)
    elif page == "Pipelines":
        render_pipelines(service)
    else:
        st.warning("Unknown page.")


if __name__ == "__main__":
    main()
