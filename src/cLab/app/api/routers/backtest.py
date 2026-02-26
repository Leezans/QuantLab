from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/backtest", tags=["backtest"])


@router.post("/run")
def run_backtest() -> dict[str, str]:
    return {"status": "not_implemented"}


@router.get("/{run_id}")
def get_backtest(run_id: str) -> dict[str, str]:
    return {"run_id": run_id, "status": "not_implemented"}
