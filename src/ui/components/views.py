from __future__ import annotations

import streamlit as st
import pandas as pd


def render_timeseries_view(df: pd.DataFrame) -> None:
    st.write("Preview")
    st.dataframe(df.head(200))

    if "close" in df.columns:
        st.write("Close")
        st.line_chart(df["close"])
    else:
        st.info("Column 'close' not found; skip chart.")
