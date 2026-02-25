from __future__ import annotations

from fastapi import APIRouter

from cLab.app.routers.binance import router as binance_router
from cLab.app.routers.system import router as system_router

api_router = APIRouter()
api_router.include_router(system_router)
api_router.include_router(binance_router)
