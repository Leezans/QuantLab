from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from quantlab.core.event_log import EventLog, LoggedEvent


class JsonlEventLog(EventLog):
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.touch(exist_ok=True)

    def append(self, event: LoggedEvent) -> None:
        data = asdict(event)
        data["occurred_at"] = event.occurred_at.isoformat()

        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

    def read_all(self) -> Iterable[LoggedEvent]:
        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                obj = json.loads(line)
                obj["occurred_at"] = datetime.fromisoformat(obj["occurred_at"])
                yield LoggedEvent(**obj)

    def next_sequence(self) -> int:
        last_sequence = 0
        for event in self.read_all():
            last_sequence = event.sequence
        return last_sequence + 1
