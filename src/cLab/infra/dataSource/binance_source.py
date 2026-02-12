from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from cLab.core.errors import DataSourceError


@dataclass(frozen=True)
class BinancePublicConfig:
    base_url: str = "https://api.binance.com"


class BinancePublicClient:
    """Public Binance REST client using stdlib urllib (no extra deps)."""

    def __init__(self, cfg: BinancePublicConfig | None = None) -> None:
        self.cfg = cfg or BinancePublicConfig()

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        qs = urllib.parse.urlencode(params or {})
        url = f"{self.cfg.base_url}{path}" + (f"?{qs}" if qs else "")
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:  # nosec B310
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            raise DataSourceError(f"binance request failed path={path} err={e}") from e

    def get_ticker_price(self, symbol: str) -> float:
        obj = self._get_json("/api/v3/ticker/price", {"symbol": symbol})
        return float(obj["price"])

    def agg_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        from_id: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "limit": int(min(1000, max(1, limit)))}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        if from_id is not None:
            params["fromId"] = int(from_id)

        obj = self._get_json("/api/v3/aggTrades", params)
        if not isinstance(obj, list):
            raise DataSourceError("Unexpected response type for aggTrades")
        return obj

    def ping(self) -> bool:
        try:
            _ = self._get_json("/api/v3/ping")
            return True
        except Exception:
            return False


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
