"""
Event Store – now dual-writes to memory + DuckDB
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum
from pydantic import BaseModel, Field
import uuid

from src.data_services.db import db


class EventType(str, Enum):
    COMMAND = "command"
    STATUS_CHANGE = "status_change"
    ALERT = "alert"
    HEALTH = "health"
    MISSION = "mission"
    DETECTION = "detection"
    INFO = "info"


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_type: EventType
    asset_id: Optional[str] = None
    message: str
    data: Dict[str, Any] = Field(default_factory=dict)
    severity: str = "info"


class EventStore:
    def __init__(self, max_events: int = 500):
        self._events: List[Event] = []
        self.max_events = max_events

    def add(self, event_type: EventType, message: str,
            asset_id: str = None, data: dict = None, severity: str = "info") -> Event:
        event = Event(
            event_type=event_type,
            message=message,
            asset_id=asset_id,
            data=data or {},
            severity=severity
        )
        self._events.append(event)

        if len(self._events) > self.max_events:
            self._events = self._events[-self.max_events:]

        # Persist to DuckDB
        try:
            db.insert_event(
                event_id=event.event_id,
                timestamp=event.timestamp,
                event_type=event.event_type.value,
                message=event.message,
                asset_id=event.asset_id,
                severity=event.severity,
                data=event.data
            )
        except Exception as e:
            print(f"[EventStore] DB write failed: {e}")

        return event

    def get_recent(self, limit: int = 30) -> List[Event]:
        return list(reversed(self._events[-limit:]))

    def get_alerts(self, limit: int = 10) -> List[Event]:
        alerts = [e for e in self._events if e.event_type == EventType.ALERT]
        return list(reversed(alerts[-limit:]))

    def get_by_asset(self, asset_id: str, limit: int = 20) -> List[Event]:
        events = [e for e in self._events if e.asset_id == asset_id]
        return list(reversed(events[-limit:]))

    def clear(self):
        self._events.clear()


event_store = EventStore()
