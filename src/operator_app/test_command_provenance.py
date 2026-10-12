"""Live GOTO: a missing Event blocks dispatch. A recorded Event links the action."""

import asyncio

import pytest
from fastapi import HTTPException

from src.data.ingest import configure
from src.data.test_ingest import MemoryDurable, MemoryLive
from src.data_services.event_store import event_store
from src.kernel.models import Command
from src.operator_app.app import CommandRequest, CommandType, platform, send_command


class _Probe:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_command(self, cmd) -> bool:
        self.sent.append(cmd.id)
        return True


def test_consumer_does_not_send_without_a_durable_event(monkeypatch):
    from src.operator_app import command_bus

    sent: list[str] = []

    class _Adapter:
        async def send_command(self, cmd) -> bool:
            sent.append(cmd.id)
            return True

    cmd = Command(
        asset_id="ugv-audit",
        command_type=CommandType.HOLD,
        issued_by="operator:op-1",
    )
    monkeypatch.setattr(command_bus, "command_audit_recorded", lambda _command_id: False)
    command_bus._lookup = lambda _asset_id: _Adapter()
    try:
        with pytest.raises(RuntimeError, match="command event was not recorded"):
            asyncio.run(command_bus._on_command(
                "cnc.command.ugv-audit",
                cmd.model_dump_json().encode(),
            ))
        assert sent == []
    finally:
        command_bus._lookup = None


def test_goto_without_event_is_not_dispatched(monkeypatch):
    asset_id = "goto-event-fail"
    probe = _Probe()
    platform.adapters[asset_id] = probe

    def boom(*_args, **_kwargs):
        raise RuntimeError("event store down")

    monkeypatch.setattr(event_store, "add", boom)
    body = CommandRequest(
        command_type=CommandType.GOTO,
        parameters={"lat": 17.4, "lon": 78.5, "alt": 40},
    )
    try:
        with pytest.raises(HTTPException) as caught:
            asyncio.run(send_command(asset_id, body))
        assert caught.value.status_code == 503
        assert probe.sent == []
        assert all(cmd.asset_id != asset_id for cmd in platform.store.commands.values())
    finally:
        platform.adapters.pop(asset_id, None)


def test_http_goto_records_command_event_and_operator_action():
    asset_id = "goto-event-ok"
    probe = _Probe()
    platform.adapters[asset_id] = probe
    configure(live=MemoryLive(), durable=MemoryDurable())
    seen = {event.event_id for event in event_store.get_by_asset(asset_id)}
    body = CommandRequest(
        command_type=CommandType.GOTO,
        parameters={"lat": 17.4, "lon": 78.5, "alt": 40},
    )
    command_id = ""
    try:
        issued = asyncio.run(send_command(asset_id, body))
        command_id = issued["id"]
        assert issued["status"] == "sent"
        assert issued["asset_id"] == asset_id
        assert issued["command_type"] == "goto"
        assert probe.sent == [command_id]
        assert platform.store.commands[command_id].id == command_id

        fresh = [event for event in event_store.get_by_asset(asset_id) if event.event_id not in seen]
        assert len(fresh) == 1
        event = fresh[0]
        assert event.asset_id == asset_id
        assert event.actor == issued["issued_by"]
        assert event.payload["command_id"] == command_id
        assert event.time.tzinfo is not None

        matches = [action for action in platform.operator_actions if action.command_id == command_id]
        assert len(matches) == 1
        action = matches[0]
        assert action.actor == issued["issued_by"]
        assert action.asset_id == asset_id
        assert action.action == "goto"
        assert action.payload == {"lat": 17.4, "lon": 78.5, "alt": 40}
        assert action.time.tzinfo is not None
    finally:
        platform.adapters.pop(asset_id, None)
        if command_id:
            platform.store.commands.pop(command_id, None)
        configure(live=None, durable=None)
