from __future__ import annotations

from dataclasses import dataclass, field

from cLab.app.services.market_data import (
    EnsureRangeResult,
    compute_volume_profile_from_parquet,
    ensure_klines_range,
    ensure_trades_range,
)


@dataclass(frozen=True)
class DataService:
    """Application service that orchestrates market-data flows for routers."""

    default_symbols: tuple[str, ...] = field(
        default_factory=lambda: ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"),
    )

    def list_symbols(self) -> list[str]:
        return list(self.default_symbols)

    def get_klines(
        self,
        *,
        symbol: str,
        start: str,
        end: str,
        interval: str,
        market: str,
        style: str,
        preview_rows: int,
        fetch_checksum: bool,
        verify_checksum: bool,
        compression: str,
        raise_on_error: bool,
    ) -> EnsureRangeResult:
        return ensure_klines_range(
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

    def get_trades(
        self,
        *,
        symbol: str,
        start: str,
        end: str,
        market: str,
        style: str,
        preview_rows: int,
        fetch_checksum: bool,
        verify_checksum: bool,
        compression: str,
        raise_on_error: bool,
    ) -> EnsureRangeResult:
        return ensure_trades_range(
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

    def get_volume_profile(
        self,
        *,
        parquet_paths: list[str],
        bins: int,
        volume_type: str,
        normalize: bool,
        start_ts: int | None = None,
        end_ts: int | None = None,
        max_rows: int | None = None,
    ) -> tuple[list[float], list[float]]:
        return compute_volume_profile_from_parquet(
            parquet_paths,
            bins=bins,
            volume_type=volume_type,
            normalize=normalize,
            start_ts=start_ts,
            end_ts=end_ts,
            max_rows=max_rows,
        )

