from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from cLab.app.api.deps import get_feature_pipeline
from cLab.app.dto import FeatureBuildRequestDTO, FeatureBuildResponseDTO
from cLab.pipelines import FeatureBuildCommand, FeaturePipeline
from cLab.core.domain.errors import DataNotFoundError, ValidationError

router = APIRouter(prefix="/api/features", tags=["features"])


@router.post("/build", response_model=FeatureBuildResponseDTO)
def build_features(
    request: FeatureBuildRequestDTO,
    pipeline: Annotated[FeaturePipeline, Depends(get_feature_pipeline)],
) -> FeatureBuildResponseDTO:
    try:
        command = FeatureBuildCommand(
            symbol=request.symbol,
            start=request.start,
            end=request.end,
            factor_set=request.factor_set,
            interval=request.interval,
            market=request.market,
            style=request.style,
            factor_params=request.factor_params,
        )
        result = pipeline.build(command)
        return FeatureBuildResponseDTO(
            artifact_path=result.artifact_path,
            row_count=result.row_count,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DataNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc
