"""Command issue writes one Event and a hot command. No fleet adapter required."""

from src.data.ingest import configure
from src.data.test_ingest import MemoryDurable, MemoryLive
from src.data_services.event_store import event_store
from src.operator_app.app import Command, CommandType, _record_command_event


def test_record_command_writes_one_event_and_hot_command():
    live = MemoryLive()
    durable = MemoryDurable()
    configure(live=live, durable=durable)
    try:
        cmd = Command(
            asset_id="ugv-01",
            command_type=CommandType.HOLD,
            issued_by="operator-1",
            status="sent",
        )
        seen = {e.event_id for e in event_store.get_by_asset(cmd.asset_id)}
        _record_command_event(cmd)
        fresh = [e for e in event_store.get_by_asset(cmd.asset_id) if e.event_id not in seen]
        assert len(fresh) == 1
        assert fresh[0].asset_id == "ugv-01"
        assert fresh[0].data["operator_id"] == "operator-1"
        assert fresh[0].actor == "operator-1"
        assert fresh[0].timestamp is not None
        assert live.objects[("command", cmd.id)]["issued_by"] == "operator-1"
        assert live.objects[("command", cmd.id)]["asset_id"] == "ugv-01"
        assert durable.commands[0]["command_id"] == cmd.id
        assert durable.commands[0]["issued_by"] == "operator-1"
    finally:
        configure(live=None, durable=None)
