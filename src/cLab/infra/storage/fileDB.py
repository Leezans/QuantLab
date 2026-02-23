# cLab/infra/storage/fileDB.py
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Literal
import zipfile
import pandas as pd

from cLab.core.config import db_cfg


class Market(str, Enum):
    SPOT = "spot"
    FUTURES = "futures"


class Frequency(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly"


class Dataset(str, Enum):
    AGGTRADES = "aggTrades"
    TRADES = "trades"
    KLINES = "klines"


KlineInterval = Literal[
    "1s",
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d",
    "1w",
    "1M",
]

AGGTRADES_COLUMNS = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "timestamp",
    "is_buyer_maker",
    "is_best_match",
]

TRADES_COLUMNS = [
    "trade_id",
    "price",
    "quantity",
    "quote_quantity",
    "timestamp",
    "is_buyer_maker",
    "is_best_match",
]

KLINES_COLUMNS = [
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
]


@dataclass(frozen=True)
class BinanceFileSpec:
    market: Market
    frequency: Frequency
    dataset: Dataset
    symbol: str
    date: str
    interval: Optional[KlineInterval] = None
    with_checksum: bool = False

    def validate(self) -> None:
        if self.dataset == Dataset.KLINES:
            if not self.interval:
                raise ValueError("KLINES requires interval.")
        else:
            if self.interval is not None:
                raise ValueError("Only KLINES supports interval.")


class LayoutStyle(str, Enum):
    MIRROR = "mirror"
    HIVE = "hive"


class BinancePathLayout:
    def __init__(self, base_path: Path, style: LayoutStyle = LayoutStyle.MIRROR):
        self.base_path = Path(base_path)
        self.style = style

    def local_path(self, spec: BinanceFileSpec) -> Path:
        spec.validate()

        if self.style == LayoutStyle.MIRROR:
            dir_path = self._mirror_dir(spec)
        elif self.style == LayoutStyle.HIVE:
            dir_path = self._hive_dir(spec)
        else:
            raise ValueError(f"Unsupported style: {self.style}")

        return dir_path / self._filename(spec)

    def ensure_parent_dir(self, spec: BinanceFileSpec) -> Path:
        p = self.local_path(spec)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _mirror_dir(self, spec: BinanceFileSpec) -> Path:
        p = self.base_path / spec.market.value / spec.frequency.value / spec.dataset.value / spec.symbol
        if spec.dataset == Dataset.KLINES:
            p = p / str(spec.interval)
        return p

    def _hive_dir(self, spec: BinanceFileSpec) -> Path:
        p = (
            self.base_path
            / "raw"
            / "exchange=binance"
            / f"market={spec.market.value}"
            / f"freq={spec.frequency.value}"
            / f"dataset={spec.dataset.value}"
            / f"symbol={spec.symbol}"
            / f"date={spec.date}"
        )
        if spec.dataset == Dataset.KLINES:
            p = p / f"interval={spec.interval}"
        return p

    def _filename(self, spec: BinanceFileSpec) -> str:
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


class FileDB:
    def __init__(self, layout: BinancePathLayout):
        self.layout = layout

    def local_path(self, spec: BinanceFileSpec) -> Path:
        return self.layout.local_path(spec)

    def exists(self, spec: BinanceFileSpec) -> bool:
        return self.local_path(spec).exists()

    def read_text(self, spec: BinanceFileSpec, encoding: str = "utf-8") -> str:
        p = self.local_path(spec)
        return p.read_text(encoding=encoding)

    def read_bytes(self, spec: BinanceFileSpec) -> bytes:
        p = self.local_path(spec)
        return p.read_bytes()

    def write_bytes_atomic(self, spec: BinanceFileSpec, data: bytes) -> Path:
        target = self.layout.ensure_parent_dir(spec)

        fd, tmp_path = tempfile.mkstemp(
            prefix=f".tmp_{target.name}.",
            dir=str(target.parent),
        )
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, target)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        return target

    def write_text_atomic(self, spec: BinanceFileSpec, text: str, encoding: str = "utf-8") -> Path:
        return self.write_bytes_atomic(spec, text.encode(encoding))
    
    def zip_to_parquet(
        self,
        zip_spec: BinanceFileSpec,
        parquet_path: Optional[Path] = None,
        delete_zip: bool = False,
        compression: str = "snappy",
    ) -> Path:
        zip_spec.validate()
        if zip_spec.with_checksum:
            raise ValueError("zip_spec.with_checksum must be False.")

        zip_path = self.local_path(zip_spec)
        if not zip_path.exists():
            raise FileNotFoundError(str(zip_path))

        if parquet_path is None:
            parquet_path = self._parquet_path_for_spec(zip_spec)

        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as z:
            members = z.namelist()
            if not members:
                raise ValueError(f"Empty zip: {zip_path}")

            csv_name = None
            for name in members:
                if name.lower().endswith(".csv"):
                    csv_name = name
                    break
            if csv_name is None:
                csv_name = members[0]

            with z.open(csv_name) as f:
                if zip_spec.dataset == Dataset.AGGTRADES:
                    df = pd.read_csv(
                        f,
                        header=None,
                        names=AGGTRADES_COLUMNS,
                        dtype={
                            "agg_trade_id": "int64",
                            "price": "float64",
                            "quantity": "float64",
                            "first_trade_id": "int64",
                            "last_trade_id": "int64",
                            "timestamp": "int64",
                            "is_buyer_maker": "bool",
                            "is_best_match": "bool",
                        },
                    )
                elif zip_spec.dataset == Dataset.TRADES:
                    df = pd.read_csv(
                        f,
                        header=None,
                        names=TRADES_COLUMNS,
                    )
                elif zip_spec.dataset == Dataset.KLINES:
                    df = pd.read_csv(
                        f,
                        header=None,
                        names=KLINES_COLUMNS,
                        dtype={
                            "open_time": "int64",
                            "open": "float64",
                            "high": "float64",
                            "low": "float64",
                            "close": "float64",
                            "volume": "float64",
                            "close_time": "int64",
                            "quote_asset_volume": "float64",
                            "number_of_trades": "int64",
                            "taker_buy_base_asset_volume": "float64",
                            "taker_buy_quote_asset_volume": "float64",
                            "ignore": "float64",
                        },
                    )
                else:
                    raise ValueError(f"Unsupported dataset: {zip_spec.dataset}")

        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            compression=compression,
            index=False,
        )

        if delete_zip:
            try:
                zip_path.unlink()
            except OSError:
                pass

        return parquet_path

    def _parquet_path_for_spec(self, spec: BinanceFileSpec) -> Path:
        zip_path = self.local_path(spec)
        return zip_path.with_suffix(".parquet")
    
    def parquet_path(self, spec: BinanceFileSpec) -> Path:
        zip_path = self.local_path(spec)
        return zip_path.with_suffix(".parquet")

    def parquet_exists(self, spec: BinanceFileSpec) -> bool:
        return self.parquet_path(spec).exists()

    def delete_artifacts(self, spec: BinanceFileSpec) -> None:
        zip_path = self.local_path(spec)
        checksum_spec = BinanceFileSpec(
            market=spec.market,
            frequency=spec.frequency,
            dataset=spec.dataset,
            symbol=spec.symbol,
            date=spec.date,
            interval=spec.interval,
            with_checksum=True,
        )
        checksum_path = self.local_path(checksum_spec)

        for p in (zip_path, checksum_path):
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass




if __name__ == "__main__":
    layout = BinancePathLayout(db_cfg.BINANCE_DIR, style=LayoutStyle.MIRROR)
    filedb = FileDB(layout)

    spec = BinanceFileSpec(
        market=Market.SPOT,
        frequency=Frequency.DAILY,
        dataset=Dataset.TRADES,
        symbol="BTCUSDT",
        date="2023-01-01",
    )
    path = filedb.local_path(spec)
    print(path)