from __future__ import annotations

from ui.services.contracts import LabService
from ui.views.trades_downloader import render_trades_downloader


def render_trades(service: LabService) -> None:
    render_trades_downloader(
        service,
        panel_key="crypto_trades",
        default_symbol="BTCUSDT",
        default_market="spot",
    )
