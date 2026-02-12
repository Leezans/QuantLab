from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class TimeSeriesPoint:
    ts: datetime
    value: float


class SQLiteTimeSeriesStore:
    """Very small SQLite-backed time-series store.

    Notes:
        - This is meant for prototyping.
        - For large scale, prefer Parquet + DuckDB.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._migrate()

    def _migrate(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ts (
                    series TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    value REAL NOT NULL,
                    PRIMARY KEY(series, ts)
                )
                """
            )

    def upsert_many(self, series: str, df: pd.DataFrame, *, ts_col: str = "ts", value_col: str = "value") -> int:
        if df.empty:
            return 0
        x = df[[ts_col, value_col]].copy()
        x[ts_col] = pd.to_datetime(x[ts_col], utc=True)
        rows = [(series, t.isoformat(), float(v)) for t, v in zip(x[ts_col], x[value_col])]
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO ts(series, ts, value) VALUES (?, ?, ?)",
                rows,
            )
            self._conn.commit()
        return len(rows)

    def load(self, series: str, *, start: datetime | None = None, end: datetime | None = None) -> pd.DataFrame:
        q = "SELECT ts, value FROM ts WHERE series=?"
        args: list[object] = [series]
        if start is not None:
            q += " AND ts >= ?"
            args.append(start.isoformat())
        if end is not None:
            q += " AND ts < ?"
            args.append(end.isoformat())
        q += " ORDER BY ts ASC"

        with self._lock:
            rows = self._conn.execute(q, args).fetchall()

        out = pd.DataFrame(rows, columns=["ts", "value"])
        if out.empty:
            return out
        out["ts"] = pd.to_datetime(out["ts"], utc=True)
        out["value"] = out["value"].astype("float64")
        return out

    def close(self) -> None:
        with self._lock:
            self._conn.close()
