from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/features", tags=["features"])


@router.post("/build")
def build_features() -> dict[str, str]:
    return {"status": "not_implemented"}
