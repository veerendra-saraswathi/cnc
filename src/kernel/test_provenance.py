"""Command provenance: a command with no matching Event is a failure."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.kernel.models import Command, CommandType, Event, EventType
from src.kernel.provenance import ProvenanceError, require_command_event


def _command() -> Command:
    return Command(
        asset_id="ugv-01",
        command_type=CommandType.HOLD,
        issued_by="operator-1",
    )


def _event(command: Command, command_id: str) -> Event:
    return Event(
        event_type=EventType.COMMAND,
        asset_id=command.asset_id,
        message="hold",
        actor=command.actor,
        data={"command_id": command_id, "operator_id": command.actor},
    )


def test_command_without_matching_event_fails():
    command = _command()
    with pytest.raises(ProvenanceError):
        require_command_event(command, [])

    other = _event(command, "not-this-command")
    with pytest.raises(ProvenanceError):
        require_command_event(command, [other])

    match = _event(command, command.id)
    found = require_command_event(command, [match])
    assert found.event_id == match.event_id
    assert found.actor == "operator-1"
    assert found.time.tzinfo is not None


def test_command_rejects_naive_time():
    with pytest.raises(ValidationError):
        Command(
            asset_id="ugv-01",
            command_type=CommandType.HOLD,
            issued_at=datetime(2026, 1, 1, 0, 0, 0),
        )
    stamped = Command(
        asset_id="ugv-01",
        command_type=CommandType.HOLD,
        issued_at=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    assert stamped.time.tzinfo is not None
