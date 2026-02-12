from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from cLab.app import api as clab_api


def _read_parquet(path: str) -> pd.DataFrame:
    try:
        import pyarrow.parquet as pq
    except Exception as e:  # pragma: no cover
        st.error(f"pyarrow is required to read parquet: {e}")
        return pd.DataFrame()

    try:
        t = pq.read_table(path)
        return t.to_pandas()
    except Exception as e:  # noqa: BLE001
        st.error(f"Failed to read parquet: {e}")
        return pd.DataFrame()


def render_clab() -> None:
    st.subheader("cLab")

    root = st.sidebar.text_input(
        "File DB Root (CLAB_FILE_DB_ROOT)",
        value=os.environ.get("CLAB_FILE_DB_ROOT", "./database/crypto"),
    )
    os.environ["CLAB_FILE_DB_ROOT"] = root

    symbol = st.sidebar.text_input("Symbol", value="BTCUSDT")
    date = st.sidebar.text_input("Date (UTC, YYYY-MM-DD)", value="2026-02-11")

    c1, c2, c3 = st.columns(3)

    with c1:
        if st.button("Fetch Ticker Price", use_container_width=True):
            r = clab_api.fetch_ticker_price(symbol=symbol, date=date)
            st.success(f"Saved: {r['out']}")
            st.json(r)

    with c2:
        if st.button("Download aggTrades (JSONL)", use_container_width=True):
            r = clab_api.download_aggtrades(symbol=symbol, date=date, max_records=5000)
            st.success(f"Saved: {r['out']} (n={r['n']})")
            st.json(r)

    with c3:
        if st.button("Build 1m Factors (Parquet)", use_container_width=True):
            r = clab_api.build_factors_1m(symbol=symbol, date=date)
            st.success(f"Saved: {r['out_path']} (rows={r['n_rows']})")
            st.json(r.__dict__ if hasattr(r, "__dict__") else r)

    st.divider()
    st.subheader("Preview")
    p = os.path.join(root, "trade_features_1m", symbol, date, "part-0000.parquet")
    st.caption(f"Preview parquet: {p}")

    df = _read_parquet(p)
    if not df.empty:
        st.dataframe(df.head(200), use_container_width=True)
        if "minute" in df.columns and "vwap" in df.columns:
            x = df.set_index("minute")
            st.line_chart(x["vwap"], use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="QuantLab", layout="wide")
    st.title("QuantLab UI")
    st.caption("Unified UI for multiple labs. Today: Streamlit prototype.")

    lab = st.sidebar.selectbox("Lab", options=["cLab"], index=0)

    if lab == "cLab":
        render_clab()


if __name__ == "__main__":
    main()
