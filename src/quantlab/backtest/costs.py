from __future__ import annotations

from quantlab.core.models import Order


class FixedBpsTransactionCostModel:
    def __init__(self, bps: float = 1.0) -> None:
        self._bps = bps

    def estimate(self, order: Order, price: float) -> float:
        return abs(order.quantity) * price * self._bps / 10_000.0

