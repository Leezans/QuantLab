from __future__ import annotations

from ui.services.contracts import LabService
from ui.views.shared.market_data import render_market_data_view


def render_kline(service: LabService) -> None:
    render_market_data_view(
        service,
        panel_key="futures_klines",
        default_symbol="IF",
        default_market="futures",
    )
