from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any

from cLab.core.domain.types import Fill, RunRecord


def serialize_run_record(record: RunRecord, *, equity_curve: list[float], fills: list[Fill]) -> dict[str, Any]:
    data = asdict(record)
    data["date_range"]["start"] = record.date_range.start.isoformat()
    data["date_range"]["end"] = record.date_range.end.isoformat()
    data["created_at"] = record.created_at.isoformat()
    data["equity_curve"] = equity_curve
    data["fills"] = [
        {
            "timestamp": fill.timestamp.isoformat(),
            "side": fill.side,
            "quantity": fill.quantity,
            "price": fill.price,
            "fee": fill.fee,
        }
        for fill in fills
    ]
    return data


def utc_now() -> datetime:
    return datetime.utcnow()

