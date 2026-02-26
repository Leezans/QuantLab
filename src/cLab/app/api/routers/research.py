from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/research", tags=["research"])


@router.get("/runs")
def list_runs() -> dict[str, list[object]]:
    return {"runs": []}
