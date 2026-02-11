from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from cLab.core.types import Fill, Order


@dataclass
class PaperAccount:
    cash: float = 10000.0
    position_qty: float = 0.0
    avg_price: float = 0.0


class PaperBroker:
    """Minimal paper broker: immediate fill at provided price."""

    def __init__(self, account: PaperAccount | None = None) -> None:
        self.account = account or PaperAccount()

    def submit(self, order: Order, *, fill_price: float) -> Fill:
        if order.qty <= 0:
            raise ValueError("qty must be > 0")

        if order.side == "buy":
            cost = order.qty * fill_price
            if cost > self.account.cash:
                raise ValueError("insufficient cash")
            self.account.cash -= cost
            new_qty = self.account.position_qty + order.qty
            if new_qty > 0:
                self.account.avg_price = (
                    self.account.avg_price * self.account.position_qty + fill_price * order.qty
                ) / new_qty
            self.account.position_qty = new_qty
        else:
            if order.qty > self.account.position_qty:
                raise ValueError("insufficient position")
            self.account.cash += order.qty * fill_price
            self.account.position_qty -= order.qty
            if self.account.position_qty == 0:
                self.account.avg_price = 0.0

        return Fill(
            order_id="paper",
            symbol=order.symbol,
            side=order.side,
            qty=order.qty,
            price=fill_price,
            ts=datetime.now(tz=timezone.utc),
        )
