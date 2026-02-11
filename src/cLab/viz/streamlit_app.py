from __future__ import annotations

import os

import pandas as pd
import streamlit as st


st.set_page_config(page_title="QuantLab", layout="wide")

st.title("QuantLab (Lightweight UI)")
st.caption("Streamlit demo UI for quick local visualization. Data sources are pluggable.")

st.sidebar.header("Data")
data_path = st.sidebar.text_input("CSV path", value=os.environ.get("CLAB_CSV", ""))

if not data_path:
    st.info("Provide a CSV path in the sidebar (or set env CLAB_CSV).")
    st.stop()

try:
    df = pd.read_csv(data_path)
except Exception as e:  # noqa: BLE001
    st.error(f"Failed to read CSV: {e}")
    st.stop()

st.subheader("Preview")
st.dataframe(df.head(200), use_container_width=True)

st.subheader("Basic charts")
num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
if not num_cols:
    st.warning("No numeric columns found.")
else:
    y = st.selectbox("Y", options=num_cols, index=0)
    st.line_chart(df[y])

st.subheader("Notes")
st.write(
    """
- This is intentionally minimal.
- For market data, connect your pipeline output to a CSV/Parquet export.
- Next step: add a dataset picker + read from a local store.
"""
)
