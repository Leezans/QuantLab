# Notebooks

This folder is for Jupyter notebooks used for exploratory research.

Recommended setup:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,viz]"

jupyter lab
```

Keep core logic in `src/cLab/` and import it from notebooks.
