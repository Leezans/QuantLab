from __future__ import annotations

from ui.services.http._base import BaseHTTPService


class FuturesHTTPService(BaseHTTPService):
    def __init__(self):
        super().__init__(
            _lab_key="futures",
            _display_name="fLab (UI Only)",
            _supports_trades_download=False,
            _symbols=["IF", "IC", "IH", "ES", "NQ"],
        )
