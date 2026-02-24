from __future__ import annotations

import streamlit as st


def render_data_status(*, source: str, total: int, cached: int, fetched: int, failed: int) -> None:
    st.caption(f"Source: {source}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total days", total)
    c2.metric("Cached", cached)
    c3.metric("Fetched", fetched)
    c4.metric("Failed", failed)


def render_errors(errors: list[str]) -> None:
    if not errors:
        return
    st.subheader("Errors")
    for item in errors:
        st.error(item)


def render_paths(paths: list[str], title: str = "Parquet Paths") -> None:
    if not paths:
        return
    st.subheader(title)
    st.write(paths)

