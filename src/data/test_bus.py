"""Subjects, in-process dead letters, and the unreachable-broker fallback."""

import ast
import asyncio
import json
from pathlib import Path

from simulations.simple_assets import create_demo_fleet
from src.data.bus import current_mode, fetch_timed_out, get_bus, reset_bus
from src.data.subjects import (
    ALERT_SUBJECT,
    DLQ_SUBJECT,
    EVENT_SUBJECT,
    MAX_AGE_SECONDS,
    MAX_BYTES,
    STREAMS,
    ack_subject,
    command_subject,
    telemetry_subject,
)
from src.data_services.event_store import event_store
from src.kernel.models import Asset, AssetType
from src.operator_app.adapters.mavlink_adapter import MAVLinkAdapter
from src.operator_app.adapters.ros2_adapter import ROS2Adapter
from src.operator_app.adapters.sim_asset_adapter import SimAssetAdapter
from src.operator_app.app import Command, CommandType, Platform
from src.operator_app.command_bus import record_ack_timeout

ROOT = Path(__file__).resolve().parents[2]


def test_empty_pull_timeout_is_not_a_fatal_error():
    class TimeoutError(Exception):
        """Stand-in for nats.errors.TimeoutError, which is not the builtin."""

    assert fetch_timed_out(TimeoutError("nats: timeout"))
    assert fetch_timed_out(__import__("builtins").TimeoutError())
    assert not fetch_timed_out(RuntimeError("broker down"))


def test_subjects_and_stream_retention():
    assert telemetry_subject("uav-01") == "cnc.telemetry.uav-01"
    assert command_subject("uav-01") == "cnc.command.uav-01"
    assert ack_subject("uav-01") == "cnc.ack.uav-01"
    assert EVENT_SUBJECT == "cnc.event"
    assert ALERT_SUBJECT == "cnc.alert"
    assert DLQ_SUBJECT == "cnc.dlq"
    assert {spec.name for spec in STREAMS} == {
        "CNC_TELEMETRY",
        "CNC_COMMAND",
        "CNC_ACK",
        "CNC_EVENT",
        "CNC_ALERT",
        "CNC_DLQ",
    }
    for spec in STREAMS:
        assert spec.max_age_seconds == MAX_AGE_SECONDS == 24 * 60 * 60
        assert spec.max_bytes == MAX_BYTES == 1024 ** 3
        assert spec.storage == "file"
        assert spec.discard == "old"


def test_bus_does_not_import_fusion_or_the_operator_app():
    tree = ast.parse((ROOT / "src/data/bus.py").read_text(encoding="utf-8"))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    joined = " ".join(modules)
    assert "ai_fusion" not in joined
    assert "operator_app" not in joined
    assert "perception" not in joined
    assert "tracking" not in joined


def test_empty_broker_error_does_not_abort_the_dial(monkeypatch):
    monkeypatch.setenv("CNC_BUS", "nats")
    reset_bus()

    async def boom(url, token=""):
        raise TimeoutError()

    async def body():
        bus = get_bus()
        monkeypatch.setattr("src.data.bus.open_connection", boom)
        assert await bus.ensure_jetstream() is False
        assert bus._gave_up is True
        assert bus._js is None
        assert bus._nc is None

    try:
        asyncio.run(body())
    finally:
        reset_bus()


def test_off_mode_does_not_open_a_connection(monkeypatch):
    monkeypatch.delenv("CNC_BUS", raising=False)
    reset_bus()

    async def body():
        bus = get_bus()
        assert current_mode() == "off"
        assert await bus.ensure_jetstream() is False
        assert bus._nc is None

    try:
        asyncio.run(body())
    finally:
        reset_bus()


def test_invalid_telemetry_is_dead_lettered_in_process():
    reset_bus()
    bus = get_bus()
    dead: list[dict] = []
    live: list[str] = []

    async def on_dlq(subject, data):
        dead.append(json.loads(data))

    async def on_telemetry(subject, data):
        live.append(subject)

    async def body():
        bus.subscribe_local("cnc.dlq", on_dlq)
        bus.subscribe_local("cnc.telemetry.>", on_telemetry)
        asset = Asset(name="Vendor", asset_type=AssetType.UAV)
        adapters = [
            SimAssetAdapter(create_demo_fleet()["uav-01"]),
            MAVLinkAdapter(asset),
            ROS2Adapter(asset),
        ]
        for adapter in adapters:
            assert await adapter.publish_vendor_telemetry({"asset_id": "bad-asset"}) is None
        assert len(dead) == 3
        assert all("position" in item["reason"] for item in dead)
        accepted = await adapters[0].publish_vendor_telemetry({
            "asset_id": "uav-01",
            "lat": 17.1,
            "lon": 78.2,
            "alt": 30,
        })
        assert accepted is not None
        assert accepted.lat == 17.1
        assert live == ["cnc.telemetry.uav-01"]

    try:
        asyncio.run(body())
    finally:
        reset_bus()


def test_reconnecting_client_resumes_without_a_new_dial(monkeypatch):
    monkeypatch.setenv("CNC_BUS", "nats")
    reset_bus()
    bus = get_bus()

    class _Client:
        is_connected = False
        is_reconnecting = True

        def jetstream(self):
            return "js-2"

    bus._nc = _Client()
    bus._js = "js-1"
    dials: list[str] = []

    async def dial(*_args, **_kwargs):
        dials.append("dial")
        raise AssertionError("reconnect must not open a second client")

    resumed: list[str] = []

    async def resume() -> None:
        resumed.append("ok")

    async def body():
        monkeypatch.setattr("src.data.bus.open_connection", dial)
        assert await bus.ensure_jetstream() is False
        assert await bus.publish_jetstream("cnc.event", {"ok": True}) is False
        assert dials == []
        bus.add_resume(resume)
        bus._nc.is_connected = True
        bus._nc.is_reconnecting = False
        await bus._on_reconnected()
        await asyncio.sleep(0)
        assert resumed == ["ok"]
        assert bus._js == "js-2"
        assert bus._gave_up is False

    try:
        asyncio.run(body())
    finally:
        bus._nc = None
        bus._js = None
        reset_bus()


def test_issue_stays_pending_when_nats_is_unreachable(monkeypatch):
    monkeypatch.setenv("CNC_BUS", "nats")
    monkeypatch.setenv("CNC_NATS_URL", "nats://127.0.0.1:1")
    reset_bus()
    sim = create_demo_fleet()["ugv-01"]
    adapter = SimAssetAdapter(sim)
    calls: list[str] = []
    original = sim.send_command

    def spy(kind, params=None):
        calls.append(kind)
        return original(kind, params)

    sim.send_command = spy
    platform = Platform()
    platform.adapters[adapter.id] = adapter
    seen = {event.event_id for event in event_store.get_by_asset(adapter.id)}

    async def body():
        try:
            cmd = Command(
                asset_id=adapter.id,
                command_type=CommandType.HOLD,
                issued_by="operator-1",
            )
            result = await platform.issue(cmd)
            assert result.status == "pending"
            assert result.result["queued"] is True
            assert calls == []
            fresh = [
                event for event in event_store.get_by_asset(adapter.id)
                if event.event_id not in seen
            ]
            assert len(fresh) == 1
            assert fresh[0].payload["command_id"] == cmd.id
            assert fresh[0].actor == "operator-1"
        finally:
            await get_bus().close()
            reset_bus()

    asyncio.run(body())


def test_ack_timeout_records_an_alert():
    reset_bus()
    platform = Platform()
    cmd = Command(
        asset_id="timeout-asset",
        command_type=CommandType.GOTO,
        issued_by="operator-1",
        parameters={"lat": 1.0, "lon": 2.0},
    )

    async def body():
        alert = await record_ack_timeout(platform, cmd)
        assert alert.title == "Command ack timeout"
        assert alert.asset_id == "timeout-asset"
        assert platform.store.alerts[alert.id].level.value == "warning"
        assert "timeout-asset" in alert.message

    try:
        asyncio.run(body())
    finally:
        reset_bus()
