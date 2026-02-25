from __future__ import annotations

from fastapi import APIRouter

from cLab.app.schemas import HealthResponseDTO

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponseDTO)
def health() -> HealthResponseDTO:
    return HealthResponseDTO(status="ok")
