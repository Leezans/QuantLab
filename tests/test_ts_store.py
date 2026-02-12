from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from cLab.infra.storage.tsDB import SQLiteTimeSeriesStore


def test_sqlite_ts_store_roundtrip(tmp_path) -> None:
    db = tmp_path / "t.sqlite"
    store = SQLiteTimeSeriesStore(str(db))

    df = pd.DataFrame(
        {
            "ts": [datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc)],
            "value": [1.0, 2.0],
        }
    )

    n = store.upsert_many("s", df)
    assert n == 2

    out = store.load("s")
    assert len(out) == 2
    assert float(out["value"].iloc[0]) == 1.0
