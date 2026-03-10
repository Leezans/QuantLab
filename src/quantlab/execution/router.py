from __future__ import annotations

from typing import Mapping, Protocol, Sequence, runtime_checkable

from quantlab.core.models import Order


@runtime_checkable
class ExecutionVenue(Protocol):
    def submit(self, orders: Sequence[Order]) -> Sequence[Order]:
        ...


class PaperBroker:
    def submit(self, orders: Sequence[Order]) -> Sequence[Order]:
        return tuple(orders)


class OrderRouter:
    def __init__(self, venues: Mapping[str, ExecutionVenue]) -> None:
        self._venues = dict(venues)

    def route(self, orders: Sequence[Order]) -> tuple[Order, ...]:
        routed: list[Order] = []
        for order in orders:
            venue = self._venues.get(order.instrument.venue)
            if venue is None:
                raise KeyError(f"no execution venue configured for {order.instrument.venue}")
            routed.extend(venue.submit((order,)))
        return tuple(routed)

