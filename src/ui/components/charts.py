from __future__ import annotations

import pandas as pd
import streamlit as st


def render_dataframe_preview(df: pd.DataFrame, rows: int = 200) -> None:
    st.dataframe(df.head(rows), use_container_width=True)


def render_close_line_chart(df: pd.DataFrame) -> None:
    if "close" not in df.columns:
        st.info("Column 'close' not found, skip line chart.")
        return
    st.line_chart(df["close"])


def render_factor_line_chart(df: pd.DataFrame) -> None:
    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        st.info("No numeric factor columns found.")
        return
    st.line_chart(numeric.tail(300))

