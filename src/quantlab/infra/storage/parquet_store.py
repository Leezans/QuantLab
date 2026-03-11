from __future__ import annotations

from pathlib import Path


class ParquetStore:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def write_dataframe(self, relative_path: str, df) -> Path:
        path = self._root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        return path

    def read_dataframe(self, relative_path: str):
        path = self._root / relative_path
        import pandas as pd
        return pd.read_parquet(path)