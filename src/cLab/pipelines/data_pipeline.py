from __future__ import annotations

from dataclasses import dataclass, field

from cLab.core.data.protocols import EnsureRangeResult, MarketDataStore


@dataclass(frozen=True)
class VolumeProfileRangeResult:
    trades_result: EnsureRangeResult
    centers: list[float]
    volumes: list[float]


@dataclass(frozen=True)
class DataPipeline:
    market_data_store: MarketDataStore
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
        return self.market_data_store.ensure_klines_range(
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
        return self.market_data_store.ensure_trades_range(
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

    def get_volume_profile_for_range(
        self,
        *,
        symbol: str,
        start: str,
        end: str,
        market: str,
        style: str,
        bins: int,
        volume_type: str,
        normalize: bool,
        start_ts: int | None = None,
        end_ts: int | None = None,
        max_rows: int | None = None,
    ) -> VolumeProfileRangeResult:
        if start_ts is not None and end_ts is not None and end_ts < start_ts:
            raise ValueError(f"end_ts < start_ts: {end_ts} < {start_ts}")

        trades_result = self.get_trades(
            symbol=symbol,
            start=start,
            end=end,
            market=market,
            style=style,
            preview_rows=1,
            fetch_checksum=True,
            verify_checksum=True,
            compression="snappy",
            raise_on_error=False,
        )

        centers, volumes = self.market_data_store.compute_volume_profile_from_parquet(
            trades_result.parquet_paths,
            bins=bins,
            volume_type=volume_type,
            normalize=normalize,
            start_ts=start_ts,
            end_ts=end_ts,
            max_rows=max_rows,
        )
        return VolumeProfileRangeResult(
            trades_result=trades_result,
            centers=centers,
            volumes=volumes,
        )

