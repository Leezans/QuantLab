# cLab/infra/storage/fileDB.py
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Literal

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