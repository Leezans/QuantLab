from __future__ import annotations

import streamlit as st


def main() -> None:
    st.set_page_config(page_title="QuantLab", layout="wide")
    st.title("QuantLab")
    st.caption("Lightweight Streamlit UI for quick visualization.")

    st.write(
        """
This is a minimal entrypoint.

Next steps:
- Connect to your data pipeline output (CSV/Parquet)
- Add charts and controls
"""
    )


if __name__ == "__main__":
    main()
