from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd

from cLab.core.domain.errors import ValidationError
from cLab.core.ports import FeatureRepository


class ParquetFeatureRepository(FeatureRepository):
    def __init__(self, base_dir: Path) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def save_features(self, symbol: str, factor_set: Sequence[str], frame: pd.DataFrame) -> str:
        symbol_norm = symbol.strip().upper()
        if not symbol_norm:
            raise ValidationError("symbol is empty")
        if frame.empty:
            raise ValidationError("feature frame is empty")

        factor_key = "-".join(sorted(factor_set)) if factor_set else "none"
        filename = f"{symbol_norm}_{factor_key}.parquet"
        path = self._base_dir / filename
        frame.to_parquet(path, index=False)
        return str(path)

