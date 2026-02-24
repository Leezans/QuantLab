from __future__ import annotations

from typing import Iterable, Tuple

import streamlit as st


def top_nav(key: str, items: Iterable[Tuple[str, str]], default: str) -> str:
    items_list = list(items)
    if not items_list:
        return default

    qp = st.query_params
    current = qp.get(key, default)

    cols = st.columns(len(items_list))
    selected = None

    for i, (label, value) in enumerate(items_list):
        is_active = value == current
        btn_label = label if not is_active else f"* {label}"
        if cols[i].button(btn_label, key=f"nav_{key}_{value}"):
            selected = value

    if selected is not None:
        st.query_params[key] = selected
        current = selected

    return current
