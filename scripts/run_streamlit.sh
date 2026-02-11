#!/usr/bin/env bash
set -euo pipefail

# Run the Streamlit UI.
#
# Usage:
#   CLAB_CSV=/path/to/data.csv ./scripts/run_streamlit.sh

python -m streamlit run src/cLab/viz/streamlit_app.py
