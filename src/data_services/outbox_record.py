"""Outbox row shared by the DuckDB and Postgres backends."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class OutboxRecord:
    id: str
    subject: str
    payload: dict
    created_at: datetime
    published_at: datetime | None
    attempts: int


def as_payload(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return loaded
    raise TypeError(f"outbox payload is not an object: {type(value).__name__}")


def issued_parts(command, event, subject: str, outbox_id: str, created_at: datetime) -> dict:
    """Column values for the command, event, and outbox inserts."""
    kind = command.command_type
    command_type = kind.value if hasattr(kind, "value") else str(kind)
    event_kind = event.event_type
    event_type = event_kind.value if hasattr(event_kind, "value") else str(event_kind)
    return {
        "command_id": command.id,
        "asset_id": command.asset_id,
        "command_type": command_type,
        "parameters": command.parameters or {},
        "issued_by": command.issued_by,
        "issued_at": command.issued_at,
        "event_id": event.event_id,
        "event_time": event.timestamp,
        "event_type": event_type,
        "event_asset_id": command.asset_id or None,
        "message": event.message,
        "severity": event.severity,
        "data": event.data or {},
        "outbox_id": outbox_id,
        "subject": subject,
        "payload": command.model_dump(mode="json"),
        "created_at": created_at,
        "status": getattr(command, "status", None) or "pending",
        "result": getattr(command, "result", None),
    }


def aware_utc(value):
    """Attach UTC to a naive timestamp. Aware values are converted to UTC."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime):
        raise TypeError(f"time value is {type(value).__name__}")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
