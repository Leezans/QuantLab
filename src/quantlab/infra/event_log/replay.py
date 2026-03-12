from __future__ import annotations

from collections.abc import Callable

from quantlab.core.event_log import EventLog, LoggedEvent

ReplayHandler = Callable[[LoggedEvent], None]


class EventReplayer:
    def __init__(self, event_log: EventLog) -> None:
        self._event_log = event_log

    def replay(self, handler: ReplayHandler) -> None:
        for event in self._event_log.read_all():
            handler(event)
