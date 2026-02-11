from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class ParquetStore:
    """Minimal Parquet writer/reader."""

    path: Path

    def write(self, df: pd.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        pq.write_table(table, tmp)
        tmp.replace(self.path)

    def read(self) -> pd.DataFrame:
        if not self.path.exists():
            return pd.DataFrame()
        return pq.read_table(self.path).to_pandas()
