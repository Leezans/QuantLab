# cLab/app/api.py
from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from cLab.infra.dataSource.binance_source import BinanceVisionClient
from cLab.pipelines.get_data import (
    PipelineOptions,
    TradesRangePipeline,
    TradesRangeRequest,
    build_default_filedb,
)
from cLab.infra.storage.fileDB import LayoutStyle, Market


def _parse_yyyy_mm_dd(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format: {s}. Expected YYYY-MM-DD.") from e


def _to_market(v: str) -> Market:
    return Market.SPOT if v == "spot" else Market.FUTURES


def _to_style(v: str) -> LayoutStyle:
    return LayoutStyle.MIRROR if v == "mirror" else LayoutStyle.HIVE


class TradesRangeIn(BaseModel):
    symbol: str = Field(..., min_length=1)
    start: str = Field(..., description="YYYY-MM-DD")
    end: str = Field(..., description="YYYY-MM-DD")
    market: Literal["spot", "futures"] = "spot"
    style: Literal["mirror", "hive"] = "mirror"

    fetch_checksum: bool = True
    verify_checksum: bool = True
    compression: str = "snappy"
    raise_on_error: bool = False


class TradesRangeOut(BaseModel):
    symbol: str
    total_days: int
    ok: int
    skipped: int
    failed: int
    parquet_paths: list[str]
    errors: list[str]


def create_app() -> FastAPI:
    app = FastAPI(title="cLab API", version="0.1.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/data/binance/trades/range", response_model=TradesRangeOut)
    def get_binance_trades_range(payload: TradesRangeIn) -> TradesRangeOut:
        try:
            req = TradesRangeRequest(
                symbol=payload.symbol.strip(),
                start_date=_parse_yyyy_mm_dd(payload.start),
                end_date=_parse_yyyy_mm_dd(payload.end),
                market=_to_market(payload.market),
            )
            opt = PipelineOptions(
                layout_style=_to_style(payload.style),
                fetch_checksum=payload.fetch_checksum,
                verify_checksum=payload.verify_checksum,
                compression=payload.compression,
                raise_on_error=payload.raise_on_error,
            )

            filedb = build_default_filedb(style=opt.layout_style)
            client = BinanceVisionClient()
            pipeline = TradesRangePipeline(filedb=filedb, client=client, options=opt)

            result = pipeline.run(req)

            return TradesRangeOut(**asdict(result))

        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e

    return app


app = create_app()