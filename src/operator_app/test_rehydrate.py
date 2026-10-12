"""Restart reads recent commands and events back into the operator process."""

import asyncio
from datetime import datetime, timedelta, timezone

from src.data_services.event_store import event_store
from src.kernel.models import CommandType, EventType
from src.operator_app.app import EntityStore, _rehydrate_operator_history


class _Memory:
    async def save_asset(self, _asset):
        return None

    async def load_assets(self):
        return []

    async def load_missions(self):
        return []

    async def load_tracks(self):
        return []

    async def load_alerts(self, _limit=30):
        return []


def test_recover_loads_commands_and_events(monkeypatch):
    when = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
    rows = [{
        "id": "cmd-rehydrate-1",
        "asset_id": "ugv-01",
        "command_type": CommandType.HOLD.value,
        "parameters": {},
        "issued_by": "operator:op-1",
        "issued_at": when,
        "status": "sent",
        "result": {"ack": True},
    }]
    events = [{
        "event_id": "evt-rehydrate-1",
        "timestamp": when + timedelta(seconds=1),
        "event_type": EventType.COMMAND.value,
        "asset_id": "ugv-01",
        "message": "hold (sent)",
        "severity": "info",
        "data": {"command_id": "cmd-rehydrate-1", "operator_id": "operator:op-1"},
        "actor": "operator:op-1",
    }]

    class _Repos:
        def list_recent_commands(self, limit=15):
            assert limit == 15
            return rows

        def list_recent_events(self, limit=500):
            assert limit == 500
            return events

    monkeypatch.setattr("src.data_services.repositories.get_repositories", lambda: _Repos())
    store = EntityStore(_Memory())

    async def body():
        await store.recover()

    before = {event.event_id for event in event_store.get_recent(500)}
    asyncio.run(body())
    try:
        loaded = store.commands["cmd-rehydrate-1"]
        assert loaded.status == "sent"
        assert loaded.result == {"ack": True}
        assert loaded.issued_by == "operator:op-1"
        fresh = [event for event in event_store.get_recent(500) if event.event_id not in before]
        assert any(event.event_id == "evt-rehydrate-1" for event in fresh)
        assert event_store.restore(events) == 0
    finally:
        store.commands.pop("cmd-rehydrate-1", None)
