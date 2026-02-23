# cLab/infra/dataSource/binance_source.py
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

import requests

from cLab.infra.storage.fileDB import (
    BinanceFileSpec,
    Dataset,
    FileDB,
)


BINANCE_VISION_BASE_URL = "https://data.binance.vision/data"


@dataclass(frozen=True)
class DownloadResult:
    data_path: str
    checksum_path: Optional[str]
    verified: bool


class BinanceVisionClient:
    def __init__(self, session: Optional[requests.Session] = None, timeout_sec: float = 60.0):
        self._session = session or requests.Session()
        self._timeout = timeout_sec

    def remote_url(self, spec: BinanceFileSpec) -> str:
        spec.validate()

        parts: list[str] = [
            BINANCE_VISION_BASE_URL,
            spec.market.value,
            spec.frequency.value,
            spec.dataset.value,
            spec.symbol,
        ]
        if spec.dataset == Dataset.KLINES:
            parts.append(str(spec.interval))

        filename = self._remote_filename(spec)
        return "/".join(parts + [filename])

    def download_to_filedb(
        self,
        filedb: FileDB,
        data_spec: BinanceFileSpec,
        fetch_checksum: bool = True,
        verify: bool = True,
    ) -> DownloadResult:
        data_spec.validate()
        if data_spec.with_checksum:
            raise ValueError("data_spec.with_checksum must be False for data download.")

        data_bytes = self._http_get(self.remote_url(data_spec))
        data_path = filedb.write_bytes_atomic(data_spec, data_bytes)

        checksum_path = None
        verified = False

        if fetch_checksum:
            checksum_spec = BinanceFileSpec(
                market=data_spec.market,
                frequency=data_spec.frequency,
                dataset=data_spec.dataset,
                symbol=data_spec.symbol,
                date=data_spec.date,
                interval=data_spec.interval,
                with_checksum=True,
            )
            checksum_text = self._http_get_text(self.remote_url(checksum_spec))
            checksum_path = filedb.write_text_atomic(checksum_spec, checksum_text)

            if verify:
                expected = self._parse_checksum_file(checksum_text)
                actual = hashlib.sha256(data_bytes).hexdigest()
                verified = (expected == actual)
                if not verified:
                    raise ValueError(
                        f"Checksum mismatch. expected={expected} actual={actual} path={data_path}"
                    )

        return DownloadResult(
            data_path=str(data_path),
            checksum_path=str(checksum_path) if checksum_path else None,
            verified=verified,
        )

    def download_and_convert(
        self,
        filedb: FileDB,
        spec: BinanceFileSpec,
        fetch_checksum: bool = True,
        verify: bool = True,
        compression: str = "snappy",
    ) -> str:
        spec.validate()

        # 1. 如果 parquet 已存在 → 跳过
        if filedb.parquet_exists(spec):
            return str(filedb.parquet_path(spec))

        # 2. 下载 zip
        self.download_to_filedb(
            filedb=filedb,
            data_spec=spec,
            fetch_checksum=fetch_checksum,
            verify=verify,
        )

        # 3. 转 parquet
        parquet_path = filedb.zip_to_parquet(
            zip_spec=spec,
            delete_zip=False,
            compression=compression,
        )

        # 4. 删除 zip + checksum
        filedb.delete_artifacts(spec)

        return str(parquet_path)
    
    def _http_get(self, url: str) -> bytes:
        r = self._session.get(url, timeout=self._timeout)
        r.raise_for_status()
        return r.content

    def _http_get_text(self, url: str, encoding: str = "utf-8") -> str:
        r = self._session.get(url, timeout=self._timeout)
        r.raise_for_status()
        r.encoding = r.encoding or encoding
        return r.text

    def _remote_filename(self, spec: BinanceFileSpec) -> str:
        if spec.dataset == Dataset.KLINES:
            name = f"{spec.symbol}-{spec.interval}-{spec.date}.zip"
        elif spec.dataset == Dataset.AGGTRADES:
            name = f"{spec.symbol}-aggTrades-{spec.date}.zip"
        elif spec.dataset == Dataset.TRADES:
            name = f"{spec.symbol}-trades-{spec.date}.zip"
        else:
            raise ValueError(f"Unsupported dataset: {spec.dataset}")

        if spec.with_checksum:
            name += ".CHECKSUM"
        return name

    def _parse_checksum_file(self, text: str) -> str:
        s = text.strip()
        if not s:
            raise ValueError("Empty checksum content.")
        token = s.split()[0].strip()
        if len(token) != 64:
            raise ValueError(f"Unexpected checksum format: {token}")
        return token.lower()



if __name__ == "__main__":
    from pathlib import Path
    import pandas as pd

    from cLab.core.config.db_cfg import load_binance_keys, BINANCE_DIR
    from cLab.infra.storage.fileDB import (
        BinancePathLayout,
        FileDB,
        Market,
        Frequency,
        LayoutStyle,
    )

    print("=== Smoke Test: Binance Download + Convert Pipeline ===")

    # 1. Load config
    keys = load_binance_keys()
    print(f"Loaded Binance API key length: {len(keys.api_key)}")

    layout = BinancePathLayout(
        base_path=BINANCE_DIR,
        style=LayoutStyle.MIRROR,
    )
    filedb = FileDB(layout=layout)
    client = BinanceVisionClient()

    # 2. Define test spec
    spec = BinanceFileSpec(
        market=Market.SPOT,
        frequency=Frequency.DAILY,
        dataset=Dataset.TRADES,
        symbol="BTCUSDT",
        date="2023-01-01",
        with_checksum=False,
    )

    # 3. Run idempotent pipeline
    parquet_path_str = client.download_and_convert(
        filedb=filedb,
        spec=spec,
        fetch_checksum=True,
        verify=True,
    )

    parquet_path = Path(parquet_path_str)
    print(f"Final parquet path: {parquet_path}")

    # 4. File existence check
    if not parquet_path.exists():
        raise RuntimeError("Parquet file not found after pipeline.")

    # 5. Read parquet
    df = pd.read_parquet(parquet_path)
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    if df.empty:
        raise RuntimeError("Parquet is empty.")

    # 6. Basic schema validation
    expected_cols = {
        Dataset.TRADES: {
            "trade_id",
            "price",
            "quantity",
            "quote_quantity",
            "timestamp",
            "is_buyer_maker",
            "is_best_match",
        },
        Dataset.AGGTRADES: {
            "agg_trade_id",
            "price",
            "quantity",
            "first_trade_id",
            "last_trade_id",
            "timestamp",
            "is_buyer_maker",
            "is_best_match",
        },
        Dataset.KLINES: {
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
        },
    }[spec.dataset]

    missing = expected_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns: {sorted(missing)}")

    # 7. Basic value sanity checks
    if "price" in df.columns:
        if (df["price"] <= 0).any():
            raise RuntimeError("Invalid price values detected.")

    if "quantity" in df.columns:
        if (df["quantity"] <= 0).any():
            raise RuntimeError("Invalid quantity values detected.")

    if "timestamp" in df.columns:
        if df["timestamp"].min() <= 0:
            raise RuntimeError("Invalid timestamp detected.")

    print("=== Smoke Test Passed ===")