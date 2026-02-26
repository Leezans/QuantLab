from __future__ import annotations

from fastapi import APIRouter

from cLab.app.api.routers.backtest import router as backtest_router
from cLab.app.api.routers.data import router as data_router
from cLab.app.api.routers.features import router as features_router
from cLab.app.api.routers.health import router as health_router
from cLab.app.api.routers.research import router as research_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(data_router)
api_router.include_router(features_router)
api_router.include_router(backtest_router)
api_router.include_router(research_router)

__all__ = ["api_router"]
