from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from quantlab.cli import ARCHITECTURE_SUMMARY, run_smoke_backtest
from quantlab.config.loader import load_settings
from quantlab.data import DataCatalog, DuckDBQueryService


@dataclass(frozen=True, slots=True)
class ApiContext:
    config_path: Path


class DatasetQueryRequest(BaseModel):
    name: str = Field(..., description="Dataset name registered in catalog")
    version: str = Field("latest", description="Dataset version or latest")
    sql: str = Field(..., description="DuckDB SQL statement")


class SmokeBacktestResponse(BaseModel):
    signals: int
    targets: int
    pnl: float
    sharpe: float
    max_drawdown: float


class ApiMetadataResponse(BaseModel):
    service: str
    version: str
    config_path: str


def _parse_smoke_backtest_metrics(payload: str) -> SmokeBacktestResponse:
    metrics: dict[str, str] = {}
    for line in payload.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        metrics[key.strip()] = value.strip()
    return SmokeBacktestResponse(
        signals=int(metrics.get("signals", "0")),
        targets=int(metrics.get("targets", "0")),
        pnl=float(metrics.get("pnl", "0")),
        sharpe=float(metrics.get("sharpe", "0")),
        max_drawdown=float(metrics.get("max_drawdown", "0")),
    )


def create_app(config_path: str = "config/base.toml") -> FastAPI:
    context = ApiContext(config_path=Path(config_path))
    app = FastAPI(title="QuantLab API", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/meta", response_model=ApiMetadataResponse)
    def metadata() -> ApiMetadataResponse:
        return ApiMetadataResponse(service="quantlab-api", version="0.1.0", config_path=str(context.config_path))

    @app.get("/architecture")
    def architecture() -> dict[str, str]:
        return {"summary": ARCHITECTURE_SUMMARY}

    @app.get("/config")
    def config_summary() -> dict[str, Any]:
        settings = load_settings(context.config_path)
        return {
            "project": asdict(settings.project),
            "storage": {key: str(value) for key, value in asdict(settings.storage).items()},
            "research": asdict(settings.research),
            "runtime": asdict(settings.runtime),
            "execution": asdict(settings.execution),
        }

    @app.get("/datasets")
    def list_datasets() -> dict[str, Any]:
        settings = load_settings(context.config_path)
        catalog = DataCatalog(settings.storage.catalog_path)
        return {"datasets": [dataset.to_dict() for dataset in catalog.list()]}

    @app.post("/datasets/query")
    def query_dataset(request: DatasetQueryRequest) -> dict[str, Any]:
        settings = load_settings(context.config_path)
        catalog = DataCatalog(settings.storage.catalog_path)
        try:
            dataset = catalog.resolve(request.name, request.version)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        rows = DuckDBQueryService(settings.storage.duckdb_path).query_dataset(dataset, request.sql)
        return {
            "dataset": dataset.to_dict(),
            "rows": [dict(row) for row in rows],
            "row_count": len(rows),
        }

    @app.post("/workflows/smoke-backtest", response_model=SmokeBacktestResponse)
    def smoke_backtest() -> SmokeBacktestResponse:
        return _parse_smoke_backtest_metrics(run_smoke_backtest())

    return app
