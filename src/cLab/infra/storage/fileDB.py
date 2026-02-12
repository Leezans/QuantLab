# cLab/infra/storage/fileDB.py
from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Literal

from cLab.core.config import db_cfg


BINANCE_VISION_BASE_URL = "https://data.binance.vision/data"
BINANCE_BASE_PATH = Path(os.path.join(db_cfg.CRYPTOSDATABASEPATH, "binance"))


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
    date: str  # "YYYY-MM-DD" for daily, "YYYY-MM" for monthly
    interval: Optional[KlineInterval] = None  # only for klines
    with_checksum: bool = False

    def validate(self) -> None:
        if self.dataset == Dataset.KLINES:
            if not self.interval:
                raise ValueError("KLINES requires interval, e.g. '1m'.")
        else:
            if self.interval is not None:
                raise ValueError("Only KLINES supports interval.")


class LayoutStyle(str, Enum):
    """
    MIRROR:
      <base>/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-02-10.zip

    HIVE:
      <base>/raw/exchange=binance/market=spot/freq=daily/dataset=aggTrades/symbol=BTCUSDT/date=2026-02-10/...
    """
    MIRROR = "mirror"
    HIVE = "hive"


class PathLayout:
    def __init__(self, base_path: Path = BINANCE_BASE_PATH, style: LayoutStyle = LayoutStyle.MIRROR):
        self.base_path = Path(base_path)
        self.style = style

    # -----------------------------
    # Public APIs
    # -----------------------------
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
        url = "/".join(parts + [filename])
        return url

    def local_path(self, spec: BinanceFileSpec) -> Path:
        spec.validate()

        if self.style == LayoutStyle.MIRROR:
            dir_path = self._mirror_dir(spec)
        elif self.style == LayoutStyle.HIVE:
            dir_path = self._hive_dir(spec)
        else:
            raise ValueError(f"Unsupported style: {self.style}")

        filename = self._remote_filename(spec)
        return dir_path / filename

    def ensure_parent_dir(self, spec: BinanceFileSpec) -> Path:
        p = self.local_path(spec)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    # -----------------------------
    # Internals: dirs
    # -----------------------------
    def _mirror_dir(self, spec: BinanceFileSpec) -> Path:
        # <base>/<market>/<frequency>/<dataset>/<symbol>[/<interval>]
        p = self.base_path / spec.market.value / spec.frequency.value / spec.dataset.value / spec.symbol
        if spec.dataset == Dataset.KLINES:
            p = p / str(spec.interval)
        return p

    def _hive_dir(self, spec: BinanceFileSpec) -> Path:
        # <base>/raw/exchange=binance/market=spot/freq=daily/dataset=aggTrades/symbol=BTCUSDT/date=YYYY-MM-DD[/interval=1m]
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

    # -----------------------------
    # Internals: file name rules
    # -----------------------------
    def _remote_filename(self, spec: BinanceFileSpec) -> str:
        """
        aggTrades:
          BTCUSDT-aggTrades-2026-02-10.zip
          BTCUSDT-aggTrades-2026-02-10.zip.CHECKSUM

        trades:
          BTCUSDT-trades-2026-02-08.zip
          BTCUSDT-trades-2026-02-08.zip.CHECKSUM

        klines (interval required):
          BTCUSDT-1m-2026-02-11.zip
          BTCUSDT-1m-2026-02-11.zip.CHECKSUM
        """
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
