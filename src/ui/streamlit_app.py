# src/ui/streamlit_app.py
from __future__ import annotations

from datetime import datetime

import streamlit as st

from cLab.pipelines.get_data import (
    PipelineOptions,
    TradesRangePipeline,
    TradesRangeRequest,
    build_default_filedb,
)
from cLab.infra.dataSource.binance_source import BinanceVisionClient
from cLab.infra.storage.fileDB import LayoutStyle, Market


def _parse_yyyy_mm_dd(s: str):
    return datetime.strptime(s, "%Y-%m-%d").date()


st.set_page_config(page_title="QuantLab - cLab Data", layout="wide")
st.title("cLab - Binance Trades Downloader")

col1, col2, col3 = st.columns(3)
with col1:
    symbol = st.text_input("Symbol", value="BTCUSDT")
with col2:
    start = st.text_input("Start (YYYY-MM-DD)", value="2023-01-01")
with col3:
    end = st.text_input("End (YYYY-MM-DD)", value="2023-01-07")

c1, c2, c3, c4 = st.columns(4)
with c1:
    market = st.selectbox("Market", ["spot", "futures"], index=0)
with c2:
    style = st.selectbox("Layout", ["mirror", "hive"], index=0)
with c3:
    verify = st.checkbox("Verify checksum", value=True)
with c4:
    cleanup = st.checkbox("Cleanup zip/checksum", value=True)

run = st.button("Run pipeline", type="primary")

if run:
    try:
        req = TradesRangeRequest(
            symbol=symbol.strip(),
            start_date=_parse_yyyy_mm_dd(start),
            end_date=_parse_yyyy_mm_dd(end),
            market=Market.SPOT if market == "spot" else Market.FUTURES,
        )

        opt = PipelineOptions(
            layout_style=LayoutStyle.MIRROR if style == "mirror" else LayoutStyle.HIVE,
            fetch_checksum=True,
            verify_checksum=verify,
            compression="snappy",
            raise_on_error=False,
        )

        filedb = build_default_filedb(style=opt.layout_style)
        client = BinanceVisionClient()
        pipeline = TradesRangePipeline(filedb=filedb, client=client, options=opt)

        with st.spinner("Running..."):
            result = pipeline.run(req)

        st.success("Done")
        st.metric("Total days", result.total_days)
        st.metric("OK", result.ok)
        st.metric("Skipped", result.skipped)
        st.metric("Failed", result.failed)

        if result.parquet_paths:
            st.subheader("Parquet paths")
            st.write(result.parquet_paths)

        if result.errors:
            st.subheader("Errors")
            for e in result.errors:
                st.error(e)

        if cleanup:
            # cleanup is already handled inside download_and_convert() if you implemented it that way
            # If not, keep this off or implement cleanup inside pipeline.
            pass

    except Exception as e:
        st.error(f"{type(e).__name__}: {e}")