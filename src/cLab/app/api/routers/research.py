from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from cLab.app.api.deps import get_research_pipeline
from cLab.app.dto import ResearchRunsResponseDTO
from cLab.pipelines import ResearchPipeline

router = APIRouter(prefix="/api/research", tags=["research"])


@router.get("/runs", response_model=ResearchRunsResponseDTO)
def list_runs(
    pipeline: Annotated[ResearchPipeline, Depends(get_research_pipeline)],
    limit: int = Query(20, ge=1, le=200),
) -> ResearchRunsResponseDTO:
    try:
        runs = pipeline.list_runs(limit=limit)
        return ResearchRunsResponseDTO(runs=runs)
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc
