from __future__ import annotations

from ui.services.http._base import BaseHTTPService


class StocksHTTPService(BaseHTTPService):
    def __init__(self):
        super().__init__(
            _lab_key="stocks",
            _display_name="sLab (UI Only)",
            _supports_trades_download=False,
            _symbols=["600519.SH", "000001.SZ", "AAPL", "MSFT"],
        )
