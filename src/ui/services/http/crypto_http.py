from __future__ import annotations

from ui.services.http._base import BaseHTTPService


class CryptoHTTPService(BaseHTTPService):
    def __init__(self):
        super().__init__(
            _lab_key="crypto",
            _display_name="cLab / CryptosLab",
            _supports_trades_download=True,
            _symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
        )
