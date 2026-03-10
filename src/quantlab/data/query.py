from __future__ import annotations

from pathlib import Path
from typing import Any

from quantlab.data.catalog import DatasetRef


class DuckDBQueryService:
    def __init__(self, database_path: str | Path = ":memory:") -> None:
        self._database_path = database_path

    def query_dataset(
        self,
        dataset: DatasetRef,
        sql: str,
        view_name: str = "dataset",
    ) -> tuple[dict[str, Any], ...]:
        duckdb = _require_duckdb()
        if isinstance(self._database_path, Path):
            self._database_path.parent.mkdir(parents=True, exist_ok=True)
            database_path = str(self._database_path)
        else:
            database_path = self._database_path
        connection = duckdb.connect(database=database_path)
        try:
            connection.execute("SET TimeZone = 'UTC'")
            parquet_glob = f"{dataset.location.as_posix()}/**/*.parquet"
            connection.execute(
                f"create or replace view {view_name} as "
                f"select * from read_parquet('{_escape_sql_literal(parquet_glob)}', hive_partitioning=true)"
            )
            cursor = connection.execute(sql)
            columns = [description[0] for description in cursor.description]
            return tuple(dict(zip(columns, row, strict=False)) for row in cursor.fetchall())
        finally:
            connection.close()

    def preview_dataset(self, dataset: DatasetRef, limit: int = 5) -> tuple[dict[str, Any], ...]:
        return self.query_dataset(dataset, f"select * from dataset order by timestamp limit {int(limit)}")


def _require_duckdb() -> Any:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError("duckdb is required for DuckDBQueryService. Install project dependencies first.") from exc
    return duckdb


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")
