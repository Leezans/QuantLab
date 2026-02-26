from __future__ import annotations

from typing import Literal

MarketLiteral = Literal["spot", "futures"]
LayoutLiteral = Literal["mirror", "hive"]
VolumeTypeLiteral = Literal["base", "quote"]

__all__ = ["LayoutLiteral", "MarketLiteral", "VolumeTypeLiteral"]

