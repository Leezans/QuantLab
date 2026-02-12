from __future__ import annotations

"""cLab API surface.

This module exposes a stable function-level API that UIs (Streamlit today,
WebUI later) can call.

We keep it thin: just orchestrate pipelines and return dicts.
"""

from cLab.pipelines.aggtrades_pipeline import BuildMinuteFactorsResult, build_minute_factors_from_aggtrades_jsonl
from cLab.pipelines.get_data import download_aggtrades_day_and_store, download_ticker_price_and_store


def fetch_ticker_price(*, symbol: str, date: str | None = None) -> dict:
    return download_ticker_price_and_store(symbol, date=date)


def download_aggtrades(*, symbol: str, date: str, max_records: int = 5000) -> dict:
    return download_aggtrades_day_and_store(symbol, date=date, max_records=max_records)


def build_factors_1m(*, symbol: str, date: str) -> BuildMinuteFactorsResult:
    return build_minute_factors_from_aggtrades_jsonl(symbol=symbol, date=date)
