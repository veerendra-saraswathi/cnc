"""
Event Store – now dual-writes to memory + DuckDB
"""

from typing import List

from src.data_services.repositories import get_repositories
from src.kernel.models import Event, EventType


class EventStore:
    def __init__(self, max_events: int = 500):
        self._events: List[Event] = []
        self.max_events = max_events

    def add(self, event_type: EventType, message: str,
            asset_id: str = None, data: dict = None, severity: str = "info",
            actor: str = None, persist: bool = True) -> Event:
        payload = dict(data or {})
        resolved_actor = actor or payload.get("operator_id") or payload.get("actor") or "system"
        event = Event(
            event_type=event_type,
            message=message,
            asset_id=asset_id,
            data=payload,
            severity=severity,
            actor=resolved_actor,
        )
        self._events.append(event)

        if len(self._events) > self.max_events:
            self._events = self._events[-self.max_events:]

        # The command transaction persists its own event. persist=False keeps the ring only.
        if persist:
            try:
                get_repositories().events.insert(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    event_type=event.event_type.value,
                    message=event.message,
                    asset_id=asset_id or None,
                    severity=event.severity,
                    data=event.data
                )
            except Exception as e:
                print(f"[EventStore] DB write failed: {e}")

        return event

    def restore(self, rows: list[dict]) -> int:
        """Load durable events into the ring. Does not write them again."""
        existing = {event.event_id for event in self._events}
        added = 0
        for row in reversed(list(rows or [])):
            event_id = str(row.get("event_id") or "")
            if not event_id or event_id in existing:
                continue
            try:
                event = Event(
                    event_id=event_id,
                    timestamp=row["timestamp"],
                    event_type=row["event_type"],
                    asset_id=row.get("asset_id") or "",
                    message=row.get("message") or "",
                    data=row.get("data") or {},
                    actor=row.get("actor") or "system",
                    severity=row.get("severity") or "info",
                )
            except Exception:
                continue
            self._events.append(event)
            existing.add(event_id)
            added += 1
        if len(self._events) > self.max_events:
            self._events = self._events[-self.max_events:]
        return added

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
