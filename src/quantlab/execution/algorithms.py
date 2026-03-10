from __future__ import annotations

from typing import Mapping, Sequence
from uuid import uuid4

from quantlab.core.enums import Side
from quantlab.core.models import Order, PortfolioSnapshot, Position, TargetPosition


class ImmediateExecutionAlgorithm:
    def create_orders(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
        marks: Mapping[str, float],
    ) -> tuple[Order, ...]:
        nav = portfolio.nav(marks) if portfolio.positions else portfolio.cash
        orders: list[Order] = []
        for target in targets:
            price = marks.get(target.instrument.symbol)
            if price is None or price <= 0:
                continue
            current_position = portfolio.positions.get(target.instrument.symbol, Position(instrument=target.instrument))
            target_quantity = (nav * target.target_weight) / price
            delta_quantity = target_quantity - current_position.quantity
            if abs(delta_quantity) < 1e-9:
                continue
            side = Side.BUY if delta_quantity > 0 else Side.SELL
            orders.append(
                Order(
                    order_id=str(uuid4()),
                    timestamp=target.as_of,
                    instrument=target.instrument,
                    side=side,
                    quantity=abs(delta_quantity),
                    metadata={"reason": target.reason, "signal_name": target.signal_name},
                )
            )
        return tuple(orders)


class TwapExecutionAlgorithm(ImmediateExecutionAlgorithm):
    def __init__(self, slices: int = 4) -> None:
        self._slices = max(1, slices)

    def create_orders(
        self,
        targets: Sequence[TargetPosition],
        portfolio: PortfolioSnapshot,
        marks: Mapping[str, float],
    ) -> tuple[Order, ...]:
        parent_orders = super().create_orders(targets, portfolio, marks)
        child_orders: list[Order] = []
        for order in parent_orders:
            child_quantity = order.quantity / self._slices
            for slice_number in range(self._slices):
                child_orders.append(
                    Order(
                        order_id=f"{order.order_id}:{slice_number}",
                        timestamp=order.timestamp,
                        instrument=order.instrument,
                        side=order.side,
                        quantity=child_quantity,
                        order_type=order.order_type,
                        limit_price=order.limit_price,
                        metadata={**order.metadata, "slice": str(slice_number), "slices": str(self._slices)},
                    )
                )
        return tuple(child_orders)

