"""Match a Command to the Event that records it."""

from __future__ import annotations

from src.kernel.models import Command, Event, EventType


class ProvenanceError(Exception):
    """A command has no Event with the same asset, actor, and command id."""


def require_command_event(command: Command, events: list[Event]) -> Event:
    """Return the Event for this command, or raise if none matches."""
    for event in events:
        if event.event_type != EventType.COMMAND:
            continue
        if event.asset_id != command.asset_id:
            continue
        if event.payload.get("command_id") != command.id:
            continue
        if event.actor != command.actor:
            continue
        if event.time.tzinfo is None or event.time.utcoffset() is None:
            raise ProvenanceError("event time is naive")
        return event
    raise ProvenanceError(f"no event for command {command.id}")
