from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True, frozen=True)
class EventEnvelope:
    event_id: str # 事件的唯一标识符，通常使用UUID生成
    event_type: str # 事件的类型，通常是事件类的名称
    payload: Any # 事件的实际内容，可以是任何类型的数据
    occurred_at: datetime # 事件发生的时间，使用UTC时间表示
    source: str | None = None # 事件的来源，可以是一个字符串标识，例如服务名称或模块名称
    correlation_id: str | None = None # 用于关联一系列相关事件的ID
    causation_id: str | None = None # 用于标识导致当前事件的事件ID
    metadata: dict[str, Any] = field(default_factory=dict) # 额外的元数据，可以包含任何辅助信息

    @classmethod
    def wrap(
        cls,
        payload: Any,
        *,
        source: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "EventEnvelope":
        return cls(
            event_id=str(uuid4()),
            event_type=type(payload).__name__,
            payload=payload,
            occurred_at=utc_now(),
            source=source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            metadata=metadata or {},
        )