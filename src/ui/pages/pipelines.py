from __future__ import annotations

import streamlit as st

from ui.services.types import LabService


def render_pipelines(service: LabService) -> None:
    st.subheader("Pipelines")

    c1, c2 = st.columns([2, 3])
    with c1:
        symbols = service.list_symbols() or ["BTCUSDT"]
        symbol = st.selectbox("Symbol", options=symbols, index=0)
        date_str = st.text_input("Date (UTC, YYYY-MM-DD)", value="2026-02-11")
        max_records = st.number_input("Max records (aggTrades safety cap)", min_value=1000, max_value=200000, value=5000, step=1000)
        run = st.button("Run: Download aggTrades -> Build 1m factors -> Build 1m bars", use_container_width=True)

    with c2:
        st.caption("This page triggers application-layer use cases via cLab.app.api.")
        st.markdown("- Storage: file db root via env `CLAB_FILE_DB_ROOT`\n- Output: aggtrades_raw (jsonl), trade_features_1m (parquet), bars_1m (parquet)")

    if run:
        with st.spinner("Running pipeline..."):
            result = service.run_pipeline_build_features(symbol=symbol, date_str=date_str, max_records=int(max_records))
        st.success("Done.")
        st.json(result)

    st.divider()
    st.subheader("Factor Eval (same day)")
    factor_col = st.text_input("Factor column", value="buy_sell_imbalance_base")
    horizon = st.number_input("Horizon (minutes)", min_value=1, max_value=1440, value=60, step=1)
    if st.button("Run factor eval", use_container_width=True):
        with st.spinner("Evaluating..."):
            out = service.run_factor_eval(symbol=symbol, date_str=date_str, factor_col=factor_col, horizon=int(horizon))
        st.json(out)
