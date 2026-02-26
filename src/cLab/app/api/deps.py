from __future__ import annotations

from functools import lru_cache

from cLab.app.services import DataService


@lru_cache(maxsize=1)
def get_data_service() -> DataService:
    return DataService()

