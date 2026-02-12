from __future__ import annotations

from typing import Dict

from ui.services.types import LabService
from ui.services.crypto_clab import CryptoCLabService


_REGISTRY: Dict[str, LabService] = {
    "crypto": CryptoCLabService(),
}


def list_labs() -> list[str]:
    return sorted(_REGISTRY.keys())


def get_lab_service(lab_key: str) -> LabService:
    if lab_key not in _REGISTRY:
        raise KeyError(f"Unknown lab: {lab_key}")
    return _REGISTRY[lab_key]
