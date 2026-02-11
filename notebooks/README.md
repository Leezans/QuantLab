# Notebooks

This folder is for Jupyter notebooks used for exploratory research.

Recommended workflow:

1. Create a virtualenv and install extras:

```bash
pip install -e ".[dev]"
pip install jupyterlab
```

2. Start JupyterLab:

```bash
jupyter lab
```

Keep core logic in `src/cLab/` and import it from notebooks.
