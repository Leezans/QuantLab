from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from cLab.app.schemas import (
    KlinesResponseDTO,
    PipelineStatsDTO,
    TradesResponseDTO,
    VolumeProfileBinDTO,
    VolumeProfileResponseDTO,
)
from cLab.app.services.market_data import (
    compute_volume_profile,
    ensure_klines_range,
    ensure_trades_range,
)

router = APIRouter(prefix="/api/binance", tags=["binance"])


@router.get("/symbols", response_model=list[str])
def list_symbols() -> list[str]:
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]


@router.get("/klines", response_model=KlinesResponseDTO)
def get_klines(
    symbol: str = Query(..., min_length=1),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    interval: str = Query("1h"),
    market: str = Query("spot", pattern="^(spot|futures)$"),
    style: str = Query("mirror", pattern="^(mirror|hive)$"),
    preview_rows: int = Query(2000, ge=1, le=20000),
    fetch_checksum: bool = True,
    verify_checksum: bool = True,
    compression: str = "snappy",
    raise_on_error: bool = False,
) -> KlinesResponseDTO:
    try:
        result = ensure_klines_range(
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            market=market,
            style=style,
            preview_rows=preview_rows,
            fetch_checksum=fetch_checksum,
            verify_checksum=verify_checksum,
            compression=compression,
            raise_on_error=raise_on_error,
        )

        preview = [
            {
                "time": int(row.time),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "volume": float(row.volume) if row.volume is not None else None,
            }
            for row in result.preview.itertuples(index=False)
        ]

        return KlinesResponseDTO(
            symbol=result.symbol,
            market=result.market,
            interval=interval,
            start=start,
            end=end,
            source=result.source,
            stats=PipelineStatsDTO(
                total_days=result.total_days,
                ok=result.ok,
                skipped=result.skipped,
                failed=result.failed,
            ),
            row_count=result.row_count,
            parquet_paths=result.parquet_paths,
            errors=result.errors,
            preview=preview,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc


@router.get("/trades", response_model=TradesResponseDTO)
def get_trades(
    symbol: str = Query(..., min_length=1),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    market: str = Query("spot", pattern="^(spot|futures)$"),
    style: str = Query("mirror", pattern="^(mirror|hive)$"),
    preview_rows: int = Query(2000, ge=1, le=50000),
    fetch_checksum: bool = True,
    verify_checksum: bool = True,
    compression: str = "snappy",
    raise_on_error: bool = False,
) -> TradesResponseDTO:
    try:
        result = ensure_trades_range(
            symbol=symbol,
            start=start,
            end=end,
            market=market,
            style=style,
            preview_rows=preview_rows,
            fetch_checksum=fetch_checksum,
            verify_checksum=verify_checksum,
            compression=compression,
            raise_on_error=raise_on_error,
        )

        preview = [
            {
                "time": int(row.time),
                "price": float(row.price),
                "quantity": float(row.quantity) if row.quantity is not None else None,
                "quote_quantity": float(row.quote_quantity) if row.quote_quantity is not None else None,
            }
            for row in result.preview.itertuples(index=False)
        ]

        return TradesResponseDTO(
            symbol=result.symbol,
            market=result.market,
            start=start,
            end=end,
            source=result.source,
            stats=PipelineStatsDTO(
                total_days=result.total_days,
                ok=result.ok,
                skipped=result.skipped,
                failed=result.failed,
            ),
            row_count=result.row_count,
            parquet_paths=result.parquet_paths,
            errors=result.errors,
            preview=preview,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc


@router.get("/volume_profile", response_model=VolumeProfileResponseDTO)
def get_volume_profile(
    symbol: str = Query(..., min_length=1),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    market: str = Query("spot", pattern="^(spot|futures)$"),
    style: str = Query("mirror", pattern="^(mirror|hive)$"),
    bins: int = Query(80, ge=10, le=300),
    volume_type: str = Query("base", pattern="^(base|quote)$"),
    normalize: bool = False,
    preview_rows: int = Query(10000, ge=1, le=50000),
) -> VolumeProfileResponseDTO:
    try:
        trades_result = ensure_trades_range(
            symbol=symbol,
            start=start,
            end=end,
            market=market,
            style=style,
            preview_rows=preview_rows,
            fetch_checksum=True,
            verify_checksum=True,
            compression="snappy",
            raise_on_error=False,
        )

        centers, volumes = compute_volume_profile(
            trades_result.preview,
            bins=bins,
            volume_type=volume_type,
            normalize=normalize,
        )
        profile = [VolumeProfileBinDTO(price=float(c), volume=float(v)) for c, v in zip(centers, volumes)]

        return VolumeProfileResponseDTO(
            symbol=trades_result.symbol,
            market=trades_result.market,
            start=start,
            end=end,
            bins=bins,
            volume_type=volume_type,
            normalized=normalize,
            profile=profile,
            trades_source=trades_result.source,
            trades_row_count=trades_result.row_count,
            errors=trades_result.errors,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc
