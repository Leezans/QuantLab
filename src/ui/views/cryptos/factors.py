from __future__ import annotations

from ui.services.contracts import LabService
from ui.views.shared.factors import render_factors_view


def render_factors(service: LabService) -> None:
    render_factors_view(
        service,
        panel_key="crypto_factors",
        default_symbol="BTCUSDT",
        default_market="spot",
    )
