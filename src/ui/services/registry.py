from __future__ import annotations

import os
from typing import Dict

from ui.services.contracts import LabService, MarketDataService

_SERVICE_MODE = os.getenv("QUANTLAB_SERVICE_MODE", "direct").strip().lower()

if _SERVICE_MODE == "direct":
    from ui.services.direct.crypto_lab import CLabService
    from ui.services.direct.futures_lab import FLabPlaceholderService
    from ui.services.direct.stocks_lab import SLabPlaceholderService

    _crypto = CLabService()
    _stocks = SLabPlaceholderService()
    _futures = FLabPlaceholderService()

    _REGISTRY: Dict[str, LabService] = {
        "stocks": _stocks,
        "crypto": _crypto,
        "futures": _futures,
    }
    _MARKET_DATA_REGISTRY: Dict[str, MarketDataService] = {
        "crypto": _crypto,
    }
elif _SERVICE_MODE == "http":
    from ui.services.http.crypto_http import CryptoHTTPService
    from ui.services.http.futures_http import FuturesHTTPService
    from ui.services.http.stocks_http import StocksHTTPService

    _crypto = CryptoHTTPService()
    _stocks = StocksHTTPService()
    _futures = FuturesHTTPService()

    _REGISTRY = {
        "stocks": _stocks,
        "crypto": _crypto,
        "futures": _futures,
    }
    _MARKET_DATA_REGISTRY = {
        "crypto": _crypto,
    }
else:
    raise ValueError(f"Unsupported QUANTLAB_SERVICE_MODE: {_SERVICE_MODE}")


def list_labs() -> list[str]:
    return sorted(_REGISTRY.keys())


def get_lab_service(lab_key: str) -> LabService:
    if lab_key not in _REGISTRY:
        raise KeyError(f"Unknown lab: {lab_key}")
    return _REGISTRY[lab_key]


def get_service_mode() -> str:
    return _SERVICE_MODE


def get_market_data_service(lab_key: str = "crypto") -> MarketDataService:
    if lab_key not in _MARKET_DATA_REGISTRY:
        raise KeyError(f"No market-data service registered for lab: {lab_key}")
    return _MARKET_DATA_REGISTRY[lab_key]
