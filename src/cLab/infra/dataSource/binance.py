from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

from cLab.core.errors import DataSourceError


@dataclass(frozen=True)
class BinancePublicConfig:
    base_url: str = "https://api.binance.com"


class BinancePublicClient:
    """Public Binance REST client using stdlib urllib (no extra deps)."""

    def __init__(self, cfg: BinancePublicConfig | None = None) -> None:
        self.cfg = cfg or BinancePublicConfig()

    def get_ticker_price(self, symbol: str) -> float:
        qs = urllib.parse.urlencode({"symbol": symbol})
        url = f"{self.cfg.base_url}/api/v3/ticker/price?{qs}"
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:  # nosec B310
                obj = json.loads(resp.read().decode("utf-8"))
            return float(obj["price"])
        except Exception as e:  # noqa: BLE001
            raise DataSourceError(f"binance ticker fetch failed: {e}") from e

    def ping(self) -> bool:
        url = f"{self.cfg.base_url}/api/v3/ping"
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:  # nosec B310
                _ = resp.read()
            return True
        except Exception:
            return False


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
