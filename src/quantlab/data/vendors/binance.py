from __future__ import annotations

import csv
import hashlib
import io
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from quantlab.core.enums import AssetClass, DataFrequency, DatasetKind, Side
from quantlab.core.models import Bar, Instrument, OrderBookLevel, OrderBookSnapshot, Quote, Trade
from quantlab.data.adapters import BinanceMarketDataAdapter
from quantlab.data.catalog import DataCatalog, DatasetRef
from quantlab.data.ingestion import IngestionRequest, build_dataset_ref
from quantlab.data.raw import RawArtifactRef, RawArtifactStore
from quantlab.data.schemas import schema_for_kind
from quantlab.data.stores import ParquetMarketDataStore

BINANCE_VISION_BASE_URL = "https://data.binance.vision/data"
BINANCE_SPOT_REST_BASE_URL = "https://api.binance.com"


class BinanceMarket(StrEnum):
    SPOT = "spot"
    FUTURES_USDM = "futures/um"
    FUTURES_COINM = "futures/cm"


class BinanceFrequency(StrEnum):
    DAILY = "daily"
    MONTHLY = "monthly"


class BinanceDataset(StrEnum):
    KLINES = "klines"
    AGGTRADES = "aggTrades"
    TRADES = "trades"


@dataclass(frozen=True, slots=True)
class BinanceHistoricalSpec:
    market: BinanceMarket
    frequency: BinanceFrequency
    dataset: BinanceDataset
    symbol: str
    date: str
    interval: str | None = None
    with_checksum: bool = False

    def validate(self) -> None:
        if not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("symbol must be a non-empty uppercase venue symbol")
        try:
            datetime.strptime(self.date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("date must use YYYY-MM-DD format") from exc
        if self.dataset is BinanceDataset.KLINES and not self.interval:
            raise ValueError("interval is required for klines")
        if self.dataset is not BinanceDataset.KLINES and self.interval is not None:
            raise ValueError("interval is only valid for klines")

    @property
    def asset_class(self) -> AssetClass:
        return AssetClass.CRYPTO

    @property
    def venue(self) -> str:
        return "BINANCE"

    @property
    def data_kind(self) -> DatasetKind:
        if self.dataset is BinanceDataset.KLINES:
            return DatasetKind.BAR
        return DatasetKind.TRADE

    @property
    def data_frequency(self) -> DataFrequency:
        if self.dataset is BinanceDataset.KLINES:
            interval = (self.interval or "").lower()
            return {
                "1m": DataFrequency.ONE_MINUTE,
                "5m": DataFrequency.FIVE_MINUTE,
                "1h": DataFrequency.HOURLY,
                "1d": DataFrequency.DAILY,
            }.get(interval, DataFrequency.DAILY)
        return DataFrequency.TICK

    def normalized_dataset_name(self) -> str:
        base_name = f"binance.{self.market.value.replace('/', '_')}.{self.dataset.value}.{self.symbol.lower()}"
        if self.dataset is BinanceDataset.KLINES and self.interval:
            return f"{base_name}.{self.interval.lower()}"
        return base_name

    def instrument(self) -> Instrument:
        return Instrument(
            symbol=self.symbol,
            venue=self.venue,
            asset_class=self.asset_class,
            quote_currency=_infer_quote_currency(self.symbol),
        )


@dataclass(frozen=True, slots=True)
class DownloadResult:
    artifact: RawArtifactRef
    checksum_path: str | None
    verified: bool


@dataclass(frozen=True, slots=True)
class HistoricalSyncReport:
    imported: tuple[DatasetRef, ...]
    existing: tuple[BinanceHistoricalSpec, ...]
    unavailable: tuple[BinanceHistoricalSpec, ...]


class HTTPSession(Protocol):
    def get(self, url: str, timeout: float):  # pragma: no cover - structural protocol
        ...


class BinanceRESTMarketDataClient:
    def __init__(
        self,
        session: HTTPSession | None = None,
        timeout_sec: float = 30.0,
        base_url: str = BINANCE_SPOT_REST_BASE_URL,
    ) -> None:
        self._session = session or _build_requests_session()
        self._timeout = timeout_sec
        self._base_url = base_url.rstrip("/")
        self._adapter = BinanceMarketDataAdapter()

    def fetch_agg_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> tuple[Trade, ...]:
        instrument = Instrument(
            symbol=symbol.upper(),
            venue="BINANCE",
            asset_class=AssetClass.CRYPTO,
            quote_currency=_infer_quote_currency(symbol.upper()),
        )
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        end_ms = int(end.astimezone(UTC).timestamp() * 1000)
        next_from_id: int | None = None
        seen_ids: set[str] = set()
        trades: list[Trade] = []

        while True:
            params: dict[str, Any] = {"symbol": instrument.symbol, "limit": min(limit, 1000)}
            if next_from_id is None:
                params["startTime"] = start_ms
                params["endTime"] = end_ms
            else:
                params["fromId"] = next_from_id
            rows = self._http_get_json("/api/v3/aggTrades", params=params)
            if not rows:
                break

            normalized = self._adapter.normalize_agg_trades(rows, instrument)
            for trade in normalized:
                if trade.timestamp < start or trade.timestamp > end:
                    continue
                if trade.trade_id in seen_ids:
                    continue
                seen_ids.add(trade.trade_id)
                trades.append(trade)

            last_row = rows[-1]
            next_from_id = int(last_row["a"]) + 1
            if int(last_row["T"]) >= end_ms or len(rows) < limit:
                break

        return tuple(sorted(trades, key=lambda trade: (trade.timestamp, trade.trade_id)))

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> tuple[Bar, ...]:
        instrument = Instrument(
            symbol=symbol.upper(),
            venue="BINANCE",
            asset_class=AssetClass.CRYPTO,
            quote_currency=_infer_quote_currency(symbol.upper()),
        )
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        end_ms = int(end.astimezone(UTC).timestamp() * 1000)
        cursor = start_ms
        bars: list[Bar] = []

        while cursor <= end_ms:
            rows = self._http_get_json(
                "/api/v3/klines",
                params={
                    "symbol": instrument.symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": min(limit, 1000),
                },
            )
            if not rows:
                break

            for row in rows:
                open_time = datetime.fromtimestamp(int(row[0]) / 1000.0, tz=UTC)
                if open_time < start or open_time > end:
                    continue
                bars.append(
                    Bar(
                        timestamp=open_time,
                        instrument=instrument,
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        metadata={
                            "interval": interval,
                            "close_time": str(row[6]),
                            "quote_asset_volume": str(row[7]),
                            "number_of_trades": str(row[8]),
                            "taker_buy_base_asset_volume": str(row[9]),
                            "taker_buy_quote_asset_volume": str(row[10]),
                            "source": "binance_rest",
                        },
                    )
                )

            last_open_time = int(rows[-1][0])
            if last_open_time >= end_ms or len(rows) < limit:
                break
            cursor = last_open_time + 1

        deduped: dict[tuple[datetime, str], Bar] = {}
        for bar in bars:
            deduped[(bar.timestamp, bar.instrument.symbol)] = bar
        return tuple(sorted(deduped.values(), key=lambda bar: bar.timestamp))

    def fetch_book_ticker(self, symbol: str, as_of: datetime | None = None) -> Quote:
        instrument = Instrument(
            symbol=symbol.upper(),
            venue="BINANCE",
            asset_class=AssetClass.CRYPTO,
            quote_currency=_infer_quote_currency(symbol.upper()),
        )
        payload = self._http_get_json("/api/v3/ticker/bookTicker", params={"symbol": instrument.symbol})
        timestamp = as_of.astimezone(UTC) if as_of else datetime.now(UTC)
        return Quote(
            timestamp=timestamp,
            instrument=instrument,
            bid_price=float(payload["bidPrice"]),
            bid_size=float(payload["bidQty"]),
            ask_price=float(payload["askPrice"]),
            ask_size=float(payload["askQty"]),
            metadata={
                "source": "binance_rest",
                "sequence_id": str(payload.get("lastUpdateId", "")),
                "timestamp_source": "gateway_receive",
            },
        )

    def fetch_order_book_snapshot(
        self,
        symbol: str,
        *,
        limit: int = 20,
        as_of: datetime | None = None,
    ) -> OrderBookSnapshot:
        instrument = Instrument(
            symbol=symbol.upper(),
            venue="BINANCE",
            asset_class=AssetClass.CRYPTO,
            quote_currency=_infer_quote_currency(symbol.upper()),
        )
        payload = self._http_get_json("/api/v3/depth", params={"symbol": instrument.symbol, "limit": limit})
        timestamp = as_of.astimezone(UTC) if as_of else datetime.now(UTC)
        bids = tuple(
            OrderBookLevel(side=Side.BUY, level=index + 1, price=float(price), quantity=float(quantity))
            for index, (price, quantity) in enumerate(payload.get("bids", ()))
        )
        asks = tuple(
            OrderBookLevel(side=Side.SELL, level=index + 1, price=float(price), quantity=float(quantity))
            for index, (price, quantity) in enumerate(payload.get("asks", ()))
        )
        return OrderBookSnapshot(
            timestamp=timestamp,
            instrument=instrument,
            sequence_id=str(payload["lastUpdateId"]),
            bids=bids,
            asks=asks,
            metadata={
                "source": "binance_rest",
                "timestamp_source": "gateway_receive",
            },
        )

    def _http_get_json(self, path: str, params: Mapping[str, Any]) -> Any:
        response = self._session.get(f"{self._base_url}{path}", params=dict(params), timeout=self._timeout)
        response.raise_for_status()
        return response.json()


class BinanceVisionClient:
    def __init__(self, session: HTTPSession | None = None, timeout_sec: float = 60.0):
        self._session = session or _build_requests_session()
        self._timeout = timeout_sec

    def remote_url(self, spec: BinanceHistoricalSpec) -> str:
        spec.validate()
        parts: list[str] = [
            BINANCE_VISION_BASE_URL,
            spec.market.value,
            spec.frequency.value,
            spec.dataset.value,
            spec.symbol,
        ]
        if spec.dataset is BinanceDataset.KLINES:
            parts.append(str(spec.interval))
        return "/".join(parts + [self._remote_filename(spec)])

    def download_to_raw_store(
        self,
        raw_store: RawArtifactStore,
        data_spec: BinanceHistoricalSpec,
        fetch_checksum: bool = True,
        verify: bool = True,
    ) -> DownloadResult:
        data_spec.validate()
        if data_spec.with_checksum:
            raise ValueError("data_spec.with_checksum must be False for data downloads")
        if verify and not fetch_checksum:
            raise ValueError("verify=True requires fetch_checksum=True")

        data_bytes = self._http_get(self.remote_url(data_spec))
        expected_sha256 = None
        checksum_relative_path = None
        checksum_url = None
        checksum_path = None

        if fetch_checksum:
            checksum_spec = BinanceHistoricalSpec(
                market=data_spec.market,
                frequency=data_spec.frequency,
                dataset=data_spec.dataset,
                symbol=data_spec.symbol,
                date=data_spec.date,
                interval=data_spec.interval,
                with_checksum=True,
            )
            checksum_url = self.remote_url(checksum_spec)
            checksum_text = self._http_get_text(checksum_url)
            expected_sha256 = self._parse_checksum_file(checksum_text)
            checksum_relative_path = self.relative_path(checksum_spec)
            checksum_path = raw_store.write_text_atomic(checksum_relative_path, checksum_text)

        actual_sha256 = hashlib.sha256(data_bytes).hexdigest()
        verified = False
        if verify and expected_sha256:
            verified = expected_sha256 == actual_sha256
            if not verified:
                if checksum_path and checksum_path.exists():
                    checksum_path.unlink()
                raise ValueError(
                    f"Checksum mismatch for {data_spec.symbol} {data_spec.date}: expected={expected_sha256} actual={actual_sha256}"
                )
        elif expected_sha256 is not None:
            verified = expected_sha256 == actual_sha256

        data_relative_path = self.relative_path(data_spec)
        data_path = raw_store.write_bytes_atomic(data_relative_path, data_bytes)
        artifact = raw_store.register(
            RawArtifactRef(
                vendor="binance",
                dataset_name=data_spec.dataset.value,
                symbol=data_spec.symbol,
                date=data_spec.date,
                data_path=data_path,
                checksum_path=checksum_path,
                source_url=self.remote_url(data_spec),
                checksum_url=checksum_url,
                sha256=actual_sha256,
                expected_sha256=expected_sha256,
                verified=verified,
                metadata={
                    "market": data_spec.market.value,
                    "frequency": data_spec.frequency.value,
                    "interval": data_spec.interval or "",
                },
            )
        )
        return DownloadResult(
            artifact=artifact,
            checksum_path=str(checksum_path) if checksum_path else None,
            verified=verified,
        )

    def relative_path(self, spec: BinanceHistoricalSpec) -> Path:
        spec.validate()
        parts = [
            "binance",
            spec.market.value,
            spec.frequency.value,
            spec.dataset.value,
            spec.symbol,
        ]
        if spec.dataset is BinanceDataset.KLINES and spec.interval:
            parts.append(spec.interval)
        return Path(*parts) / self._remote_filename(spec)

    def _http_get(self, url: str) -> bytes:
        response = self._session.get(url, timeout=self._timeout)
        response.raise_for_status()
        return response.content

    def _http_get_text(self, url: str, encoding: str = "utf-8") -> str:
        response = self._session.get(url, timeout=self._timeout)
        response.raise_for_status()
        response.encoding = response.encoding or encoding
        return response.text

    def _remote_filename(self, spec: BinanceHistoricalSpec) -> str:
        if spec.dataset is BinanceDataset.KLINES:
            filename = f"{spec.symbol}-{spec.interval}-{spec.date}.zip"
        elif spec.dataset is BinanceDataset.AGGTRADES:
            filename = f"{spec.symbol}-aggTrades-{spec.date}.zip"
        elif spec.dataset is BinanceDataset.TRADES:
            filename = f"{spec.symbol}-trades-{spec.date}.zip"
        else:  # pragma: no cover - exhaustive by enum
            raise ValueError(f"unsupported Binance dataset: {spec.dataset}")
        if spec.with_checksum:
            filename += ".CHECKSUM"
        return filename

    def _parse_checksum_file(self, text: str) -> str:
        token = text.strip().split()[0].strip()
        if len(token) != 64:
            raise ValueError(f"unexpected checksum format: {token}")
        return token.lower()


class BinanceHistoricalParser:
    TRADE_COLUMNS = (
        "trade_id",
        "price",
        "quantity",
        "quote_quantity",
        "timestamp",
        "is_buyer_maker",
        "is_best_match",
    )
    AGGTRADE_COLUMNS = (
        "agg_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "timestamp",
        "is_buyer_maker",
        "is_best_match",
    )
    KLINE_COLUMNS = (
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    )

    def parse_trades(self, zip_path: Path, spec: BinanceHistoricalSpec) -> tuple[Trade, ...]:
        if spec.dataset not in {BinanceDataset.TRADES, BinanceDataset.AGGTRADES}:
            raise ValueError(f"trade parser does not support dataset {spec.dataset}")
        instrument = spec.instrument()
        column_names = self.TRADE_COLUMNS if spec.dataset is BinanceDataset.TRADES else self.AGGTRADE_COLUMNS
        output: list[Trade] = []
        for row in self._iter_csv_rows(zip_path, column_names):
            buyer_maker = _parse_bool(row["is_buyer_maker"])
            metadata = {
                "binance_dataset": spec.dataset.value,
                "is_best_match": str(_parse_bool(row["is_best_match"])).lower(),
            }
            if spec.dataset is BinanceDataset.TRADES:
                metadata["quote_quantity"] = str(float(row["quote_quantity"]))
                trade_id = str(row["trade_id"])
            else:
                metadata["first_trade_id"] = str(row["first_trade_id"])
                metadata["last_trade_id"] = str(row["last_trade_id"])
                trade_id = str(row["agg_trade_id"])
            output.append(
                Trade(
                    timestamp=_from_epoch_millis(row["timestamp"]),
                    instrument=instrument,
                    trade_id=trade_id,
                    price=float(row["price"]),
                    quantity=float(row["quantity"]),
                    side=Side.SELL if buyer_maker else Side.BUY,
                    metadata=metadata,
                )
            )
        return tuple(sorted(output, key=lambda trade: (trade.timestamp, trade.trade_id)))

    def parse_bars(self, zip_path: Path, spec: BinanceHistoricalSpec) -> tuple[Bar, ...]:
        if spec.dataset is not BinanceDataset.KLINES:
            raise ValueError(f"bar parser does not support dataset {spec.dataset}")
        instrument = spec.instrument()
        output: list[Bar] = []
        for row in self._iter_csv_rows(zip_path, self.KLINE_COLUMNS):
            output.append(
                Bar(
                    timestamp=_from_epoch_millis(row["open_time"]),
                    instrument=instrument,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                    metadata={
                        "close_time": str(row["close_time"]),
                        "quote_asset_volume": str(row["quote_asset_volume"]),
                        "number_of_trades": str(row["number_of_trades"]),
                        "taker_buy_base_asset_volume": str(row["taker_buy_base_asset_volume"]),
                        "taker_buy_quote_asset_volume": str(row["taker_buy_quote_asset_volume"]),
                    },
                )
            )
        return tuple(sorted(output, key=lambda bar: bar.timestamp))

    def _iter_csv_rows(self, zip_path: Path, expected_columns: Sequence[str]) -> Sequence[dict[str, str]]:
        with zipfile.ZipFile(zip_path) as archive:
            csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
            if len(csv_names) != 1:
                raise ValueError(f"expected exactly one CSV file in {zip_path}, found {csv_names}")
            with archive.open(csv_names[0], "r") as handle:
                text_stream = io.TextIOWrapper(handle, encoding="utf-8")
                reader = csv.reader(text_stream)
                try:
                    first_row = next(reader)
                except StopIteration:
                    return ()
                rows = []
                if _looks_like_header(first_row, expected_columns):
                    header = tuple(first_row)
                else:
                    header = tuple(expected_columns)
                    rows.append(_row_to_dict(first_row, expected_columns))
                for row in reader:
                    rows.append(_row_to_dict(row, header))
                return tuple(rows)


class BinanceHistoricalImporter:
    def __init__(
        self,
        raw_store: RawArtifactStore,
        dataset_store: ParquetMarketDataStore,
        catalog: DataCatalog,
        client: BinanceVisionClient | None = None,
        parser: BinanceHistoricalParser | None = None,
    ) -> None:
        self._raw_store = raw_store
        self._dataset_store = dataset_store
        self._catalog = catalog
        self._client = client or BinanceVisionClient()
        self._parser = parser or BinanceHistoricalParser()

    def ingest(
        self,
        spec: BinanceHistoricalSpec,
        normalized_base_path: Path,
        dataset_name: str | None = None,
        fetch_checksum: bool = True,
        verify: bool = True,
    ) -> DatasetRef:
        spec.validate()
        artifact = self._client.download_to_raw_store(
            raw_store=self._raw_store,
            data_spec=spec,
            fetch_checksum=fetch_checksum,
            verify=verify,
        ).artifact
        request = IngestionRequest(
            dataset_name=dataset_name or spec.normalized_dataset_name(),
            version=spec.date,
            instrument=spec.instrument(),
            start=datetime.strptime(spec.date, "%Y-%m-%d").replace(tzinfo=UTC),
            end=datetime.strptime(spec.date, "%Y-%m-%d").replace(tzinfo=UTC),
            frequency=spec.data_frequency,
            storage_path=normalized_base_path,
        )

        if spec.data_kind is DatasetKind.BAR:
            bars = self._parser.parse_bars(artifact.data_path, spec)
            dataset = build_dataset_ref(request=request, data_kind=DatasetKind.BAR, row_count=len(bars))
            dataset = DatasetRef(
                name=dataset.name,
                version=dataset.version,
                data_kind=dataset.data_kind,
                asset_class=dataset.asset_class,
                location=dataset.location,
                schema=schema_for_kind(DatasetKind.BAR).columns,
                storage_tier=dataset.storage_tier,
                row_count=dataset.row_count,
                format=dataset.format,
                partition_columns=dataset.partition_columns,
                created_at=dataset.created_at,
                metadata={**dataset.metadata, "raw_path": str(artifact.data_path), "source": "binance_vision"},
            )
            self._dataset_store.write_bars(dataset, bars)
        else:
            trades = self._parser.parse_trades(artifact.data_path, spec)
            dataset = build_dataset_ref(request=request, data_kind=DatasetKind.TRADE, row_count=len(trades))
            dataset = DatasetRef(
                name=dataset.name,
                version=dataset.version,
                data_kind=dataset.data_kind,
                asset_class=dataset.asset_class,
                location=dataset.location,
                schema=schema_for_kind(DatasetKind.TRADE).columns,
                storage_tier=dataset.storage_tier,
                row_count=dataset.row_count,
                format=dataset.format,
                partition_columns=dataset.partition_columns,
                created_at=dataset.created_at,
                metadata={**dataset.metadata, "raw_path": str(artifact.data_path), "source": "binance_vision"},
            )
            self._dataset_store.write_trades(dataset, trades)

        self._catalog.register(dataset)
        return dataset


class BinanceHistoryEnsurer:
    def __init__(
        self,
        raw_store: RawArtifactStore,
        dataset_store: ParquetMarketDataStore,
        catalog: DataCatalog,
        client: BinanceVisionClient | None = None,
        parser: BinanceHistoricalParser | None = None,
    ) -> None:
        self._catalog = catalog
        self._importer = BinanceHistoricalImporter(
            raw_store=raw_store,
            dataset_store=dataset_store,
            catalog=catalog,
            client=client,
            parser=parser,
        )

    def ensure_range(
        self,
        *,
        symbols: Sequence[str],
        start_date: str,
        end_date: str,
        normalized_base_path: Path,
        interval: str | None = None,
        market: BinanceMarket = BinanceMarket.SPOT,
        frequency: BinanceFrequency = BinanceFrequency.DAILY,
        datasets: Sequence[BinanceDataset] = (BinanceDataset.KLINES, BinanceDataset.AGGTRADES),
        fetch_checksum: bool = True,
        verify: bool = True,
    ) -> HistoricalSyncReport:
        imported: list[DatasetRef] = []
        existing: list[BinanceHistoricalSpec] = []
        unavailable: list[BinanceHistoricalSpec] = []
        for current_date in _iter_calendar_dates(start_date, end_date):
            date_str = current_date.isoformat()
            for symbol in dict.fromkeys(item.upper() for item in symbols):
                for dataset in dict.fromkeys(datasets):
                    spec = BinanceHistoricalSpec(
                        market=market,
                        frequency=frequency,
                        dataset=dataset,
                        symbol=symbol,
                        date=date_str,
                        interval=interval if dataset is BinanceDataset.KLINES else None,
                    )
                    if self._is_available(spec):
                        existing.append(spec)
                        continue
                    try:
                        imported.append(
                            self._importer.ingest(
                                spec=spec,
                                normalized_base_path=normalized_base_path,
                                fetch_checksum=fetch_checksum,
                                verify=verify,
                            )
                        )
                    except Exception as exc:
                        if _http_status_code(exc) == 404:
                            unavailable.append(spec)
                            continue
                        raise
        return HistoricalSyncReport(
            imported=tuple(imported),
            existing=tuple(existing),
            unavailable=tuple(unavailable),
        )

    def _is_available(self, spec: BinanceHistoricalSpec) -> bool:
        dataset = self._catalog.get(spec.normalized_dataset_name(), spec.date)
        if dataset is None:
            return False
        if dataset.location.exists() and any(dataset.location.rglob("*.parquet")):
            return True
        return False


def _row_to_dict(row: Sequence[str], header: Sequence[str]) -> dict[str, str]:
    if len(row) != len(header):
        raise ValueError(f"unexpected column count. expected={len(header)} actual={len(row)} row={row}")
    return {str(column): str(value) for column, value in zip(header, row, strict=True)}


def _looks_like_header(first_row: Sequence[str], expected_columns: Sequence[str]) -> bool:
    normalized_first_row = tuple(value.strip() for value in first_row)
    return normalized_first_row == tuple(expected_columns)


def _from_epoch_millis(value: str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000.0, tz=UTC)


def _parse_bool(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f"unsupported boolean value: {value}")


def _build_requests_session() -> HTTPSession:
    try:
        import requests
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised only without dependency
        raise RuntimeError("requests is required for BinanceVisionClient. Install project dependencies first.") from exc
    return requests.Session()


def _infer_quote_currency(symbol: str) -> str:
    normalized = symbol.upper()
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USD", "BTC", "ETH", "BNB", "EUR"):
        if normalized.endswith(suffix):
            return suffix
    return "USD"


def _iter_calendar_dates(start_date: str, end_date: str) -> tuple[date, ...]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end < start:
        raise ValueError(f"end_date must be >= start_date. start_date={start_date} end_date={end_date}")
    current = start
    output: list[date] = []
    while current <= end:
        output.append(current)
        current += timedelta(days=1)
    return tuple(output)


def _http_status_code(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    return int(status_code) if status_code is not None else None
