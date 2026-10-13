"""Outbox commit, relay restart, duplicate delivery, and a down broker."""

from __future__ import annotations

import asyncio
import json

from simulations.simple_assets import create_demo_fleet
from src.data.bus import MessageBus, get_bus, reset_bus
from src.data.outbox import backoff_seconds, get_relay, publish_pending, reset_relay
from src.data.sequence import live_telemetry_stale, remember_live_telemetry
from src.data_services.db import db
from src.kernel.models import Command, CommandType, Telemetry
from src.operator_app.adapters.idempotency import reset_processed
from src.operator_app.adapters.sim_asset_adapter import SimAssetAdapter
from src.operator_app.app import Platform
from src.operator_app.command_bus import _on_ack


def test_backoff_grows_and_caps():
    assert backoff_seconds(0) == 0.0
    assert backoff_seconds(2) > backoff_seconds(1)
    assert backoff_seconds(20) == 5.0


def test_in_process_issue_marks_the_outbox_and_sequences_commands(monkeypatch):
    monkeypatch.delenv("CNC_BUS", raising=False)
    reset_bus()
    reset_relay()
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

    async def body():
        first = await platform.issue(Command(
            asset_id=adapter.id,
            command_type=CommandType.HOLD,
            issued_by="operator-1",
        ))
        second = await platform.issue(Command(
            asset_id=adapter.id,
            command_type=CommandType.HOLD,
            issued_by="operator-1",
        ))
        assert first.status == "sent"
        assert second.status == "sent"
        assert first.seq >= 1
        assert second.seq == first.seq + 1
        assert calls == ["stop", "stop"]
        assert db.command_published(first.id)
        assert db.command_published(second.id)

    asyncio.run(body())


def test_publish_sets_the_command_dedupe_header(monkeypatch):
    monkeypatch.setenv("CNC_BUS", "nats")
    reset_bus()
    bus = get_bus()
    seen: dict = {}

    class _JetStream:
        async def publish(self, subject, payload, headers=None, **kwargs):
            seen["subject"] = subject
            seen["headers"] = headers
            return object()

    bus._js = _JetStream()
    bus._nc = object()

    async def body():
        assert await bus.publish_jetstream(
            "cnc.command.uav-01",
            {"id": "cmd-1"},
            msg_id="cmd-1",
        )

    try:
        asyncio.run(body())
    finally:
        bus._nc = None
        reset_bus()
    assert seen["subject"] == "cnc.command.uav-01"
    assert seen["headers"]["Nats-Msg-Id"] == "cmd-1"
    assert isinstance(bus, MessageBus)


def test_live_snapshot_drops_an_older_sequence():
    asset_id = "seq-live"
    inserted: list[str] = []

    class _Telemetry:
        def insert(self, **kwargs):
            inserted.append(kwargs["asset_id"])

    class _Repos:
        telemetry = _Telemetry()

    class _Snap:
        def __init__(self):
            self.seqs: list[int] = []

        def put(self, kind, object_id, payload):
            assert kind == "platform"
            assert object_id == asset_id
            self.seqs.append(payload["seq"])

    snap = _Snap()
    import src.data.ingest_consumer as ingest_consumer
    original_repos = ingest_consumer.get_repositories
    original_snap = ingest_consumer._snapshot
    ingest_consumer.get_repositories = lambda: _Repos()
    ingest_consumer._snapshot = lambda: snap
    try:
        samples = [
            Telemetry(asset_id=asset_id, seq=2, lat=17.2, lon=78.2, alt=10),
            Telemetry(asset_id=asset_id, seq=1, lat=17.1, lon=78.1, alt=10),
            Telemetry(asset_id=asset_id, seq=3, lat=17.3, lon=78.3, alt=10),
        ]

        async def body():
            for sample in samples:
                raw = json.dumps(sample.model_dump(mode="json")).encode()
                await ingest_consumer.handle_telemetry(f"cnc.telemetry.{asset_id}", raw)

        asyncio.run(body())
    finally:
        ingest_consumer.get_repositories = original_repos
        ingest_consumer._snapshot = original_snap
    assert inserted == [asset_id, asset_id, asset_id]
    assert snap.seqs == [2, 3]
    assert live_telemetry_stale(asset_id, 3)
    assert not live_telemetry_stale(asset_id, 4)
    remember_live_telemetry(asset_id, 4)


def _arm_bus(monkeypatch):
    monkeypatch.setenv("CNC_BUS", "nats")
    monkeypatch.delenv("CNC_NATS_URL", raising=False)
    reset_bus()
    reset_relay()
    reset_processed()

    async def refuse(self, subject, payload, msg_id=None, timeout=None):
        return False

    monkeypatch.setattr(MessageBus, "publish_jetstream", refuse)


def _platform():
    sim = create_demo_fleet()["ugv-02"]
    adapter = SimAssetAdapter(sim)
    calls: list[tuple] = []
    original = sim.send_command

    def spy(kind, params=None):
        calls.append((kind, dict(params or {})))
        return original(kind, params)

    sim.send_command = spy
    platform = Platform()
    platform.adapters[adapter.id] = adapter
    return platform, adapter, sim, calls


def test_crash_before_publish_executes_once_after_restart(monkeypatch):
    _arm_bus(monkeypatch)
    platform, adapter, sim, calls = _platform()
    relay = get_relay()
    from src.data import outbox as outbox_mod

    async def deliver(subject, payload, msg_id):
        if payload.get("id") != expected["id"]:
            return True
        assert msg_id == payload["id"]
        await adapter.send_command(Command.model_validate(payload))
        return True

    relay.set_publisher(deliver)
    expected = {"id": ""}
    original = outbox_mod.publish_pending

    async def killed(*, force=False):
        raise RuntimeError("killed before publish")

    async def body():
        cmd = Command(
            asset_id=adapter.id,
            command_type=CommandType.GOTO,
            issued_by="operator-1",
            parameters={"lat": sim.position.lat + 0.01, "lon": sim.position.lon, "alt": 5},
        )
        expected["id"] = cmd.id
        outbox_mod.publish_pending = killed
        try:
            try:
                await platform.issue(cmd)
            except RuntimeError as exc:
                assert "killed" in str(exc)
            else:
                raise AssertionError("issue returned after the crash")
        finally:
            outbox_mod.publish_pending = original
        assert calls == []
        assert db.command_published(cmd.id) is False
        await publish_pending(force=True)
        assert len(calls) == 1
        await publish_pending(force=True)
        assert len(calls) == 1
        assert sim.target is not None

    try:
        asyncio.run(body())
    finally:
        outbox_mod.publish_pending = original
        reset_relay()
        reset_bus()


def test_duplicate_publish_executes_once_and_a_down_broker_stays_pending(monkeypatch):
    _arm_bus(monkeypatch)
    platform, adapter, sim, calls = _platform()
    relay = get_relay()
    bus = get_bus()
    up = {"ok": False}
    expected = {"id": ""}
    hits = {"n": 0}

    async def on_ack(subject, data):
        await _on_ack(subject, data)

    bus.subscribe_local("cnc.ack.>", on_ack)

    async def deliver(subject, payload, msg_id):
        if payload.get("id") != expected["id"]:
            return True
        assert msg_id == expected["id"]
        if not up["ok"]:
            return False
        hits["n"] += 1
        await adapter.send_command(Command.model_validate(payload))
        return True

    relay.set_publisher(deliver)

    async def body():
        cmd = Command(
            asset_id=adapter.id,
            command_type=CommandType.GOTO,
            issued_by="operator-1",
            parameters={"lat": sim.position.lat + 0.02, "lon": sim.position.lon, "alt": 4},
        )
        expected["id"] = cmd.id
        result = await platform.issue(cmd)
        assert result.status == "pending"
        assert result.result["queued"] is True
        assert calls == []
        assert db.command_published(cmd.id) is False

        up["ok"] = True
        await publish_pending(force=True)
        assert hits["n"] == 1
        assert len(calls) == 1
        assert result.status == "sent"
        assert sim.target is not None
        first_target = (sim.target.lat, sim.target.lon)

        db.con.execute(
            """
            UPDATE outbox SET published_at = NULL
            WHERE json_extract_string(payload, '$.id') = ?
            """,
            [cmd.id],
        )
        await publish_pending(force=True)
        assert hits["n"] == 2
        await adapter.send_command(cmd)
        assert len(calls) == 1
        assert (sim.target.lat, sim.target.lon) == first_target

    try:
        asyncio.run(body())
    finally:
        reset_relay()
        reset_bus()
