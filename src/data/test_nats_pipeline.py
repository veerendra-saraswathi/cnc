"""JetStream ingest. Skipped when NATS, Postgres, or Redis does not answer."""

from __future__ import annotations

import asyncio
import json
import time
from uuid import uuid4

import pytest
from sqlalchemy import text

from simulations.simple_assets import create_demo_fleet
from src.data.bus import get_bus, reset_bus
from src.data.pipeline import start_pipeline, stop_pipeline
from src.data.redis_live import RedisLiveStore
from src.data.subjects import STREAMS
from src.data_services.event_store import event_store
from src.kernel.models import Asset, AssetType
from src.operator_app.adapters.mavlink_adapter import MAVLinkAdapter
from src.operator_app.adapters.sim_asset_adapter import SimAssetAdapter
from src.operator_app.app import Command, CommandType, Platform
from src.operator_app.command_bus import start_command_bridge, stop_command_bridge

pytestmark = pytest.mark.nats

_STACK = {"done": False, "error": None}
_DURABLES = (
    ("CNC_TELEMETRY", "cnc-ingest"),
    ("CNC_TELEMETRY", "cnc-fusion"),
    ("CNC_EVENT", "cnc-ingest-event"),
    ("CNC_ALERT", "cnc-ingest-alert"),
    ("CNC_COMMAND", "cnc-commands"),
    ("CNC_ACK", "cnc-acks"),
)


def _stack_error() -> str | None:
    if _STACK["done"]:
        return _STACK["error"]
    _STACK["done"] = True
    problems: list[str] = []
    try:
        asyncio.run(_probe_nats())
    except Exception as exc:
        problems.append(f"NATS {exc.__class__.__name__}: {str(exc).splitlines()[0]}")
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        from src.data_services.postgres_backend import admin_url

        engine = create_engine(admin_url(), poolclass=NullPool, connect_args={"connect_timeout": 2})
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        finally:
            engine.dispose()
    except Exception as exc:
        problems.append(f"Postgres {exc.__class__.__name__}: {str(exc).splitlines()[0]}")
    try:
        RedisLiveStore().client.ping()
    except Exception as exc:
        problems.append(f"Redis {exc.__class__.__name__}: {str(exc).splitlines()[0]}")
    _STACK["error"] = "; ".join(problems) if problems else None
    return _STACK["error"]


def _require_stack() -> None:
    error = _stack_error()
    if error:
        pytest.skip(f"JetStream stack is not reachable: {error}")


async def _probe_nats() -> None:
    from src.data.bus import open_connection
    from src.data_services.settings import setting

    connection = await open_connection(
        setting("CNC_NATS_URL", "nats://127.0.0.1:4222"),
        setting("NATS_TOKEN", ""),
    )
    try:
        await connection.jetstream().account_info()
    finally:
        await connection.close()


def _arm(monkeypatch) -> None:
    _require_stack()
    monkeypatch.setenv("CNC_BUS", "nats")
    monkeypatch.setenv("STORAGE_BACKEND", "postgres")
    reset_bus()


def _enum_value(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _row(sql: str, **params):
    from src.data_services.postgres_backend import admin_engine

    with admin_engine().connect() as conn:
        return conn.execute(text(sql), params).first()


def _loads(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


async def _drop_durables() -> None:
    from nats.js.errors import NotFoundError

    bus = get_bus()
    assert await bus.ensure_jetstream()
    for stream, durable in _DURABLES:
        try:
            await bus.js.delete_consumer(stream, durable)
        except NotFoundError:
            pass


async def _shutdown() -> None:
    await stop_command_bridge()
    await stop_pipeline()
    await get_bus().close()
    reset_bus()


@pytest.mark.nats
def test_streams_use_the_architecture_retention(monkeypatch):
    _arm(monkeypatch)

    async def body():
        try:
            bus = get_bus()
            assert await bus.ensure_jetstream()
            for spec in STREAMS:
                info = await bus.js.stream_info(spec.name)
                assert list(info.config.subjects) == list(spec.subjects)
                assert info.config.max_bytes == spec.max_bytes
                assert abs(float(info.config.max_age) - spec.max_age_seconds) < 1
                assert _enum_value(info.config.storage) == "file"
                assert _enum_value(info.config.discard) == "old"
                assert _enum_value(info.config.retention) == "limits"
        finally:
            await _shutdown()

    asyncio.run(body())


@pytest.mark.nats
def test_sim_telemetry_reaches_postgres_redis_and_fusion(monkeypatch):
    _arm(monkeypatch)

    async def body():
        adapter = SimAssetAdapter(create_demo_fleet()["uav-01"])
        try:
            await adapter.start()
            await _drop_durables()
            assert await start_pipeline()
            sample = await adapter.get_telemetry()
            assert sample is not None
            asset_id = sample.asset_id

            def _look():
                # Sync clients must not run on the consumer loop.
                return (
                    _row(
                        "SELECT lat, lon FROM telemetry WHERE asset_id = :asset_id ORDER BY time DESC LIMIT 1",
                        asset_id=asset_id,
                    ),
                    _row(
                        "SELECT track_id, lat FROM tracks WHERE asset_id = :asset_id",
                        asset_id=asset_id,
                    ),
                    _row(
                        "SELECT detection_id FROM detections WHERE asset_id = :asset_id LIMIT 1",
                        asset_id=asset_id,
                    ),
                    _row(
                        "SELECT alert_id, level FROM alerts WHERE asset_id = :asset_id",
                        asset_id=asset_id,
                    ),
                    RedisLiveStore().get("platform", asset_id),
                )

            deadline = time.monotonic() + 10
            telem = track = detection = alert = live = None
            while time.monotonic() < deadline:
                telem, track, detection, alert, live = await asyncio.to_thread(_look)
                if telem and track and detection and alert and live.get("asset_id") == asset_id:
                    break
                await asyncio.sleep(0.2)
            assert telem is not None, "postgres telemetry missing"
            assert float(telem[0]) == pytest.approx(sample.lat)
            assert track is not None, "fusion track missing"
            assert track[0] == f"own-{asset_id}"
            assert detection is not None, "fusion detection missing"
            assert alert is not None, "fusion alert missing"
            assert live["asset_id"] == asset_id
            assert live["position"]["lat"] == pytest.approx(sample.lat)
        finally:
            await _shutdown()

    asyncio.run(body())


@pytest.mark.nats
def test_goto_is_dispatched_acked_and_audited(monkeypatch):
    _arm(monkeypatch)

    async def body():
        sim = create_demo_fleet()["ugv-01"]
        adapter = SimAssetAdapter(sim)
        platform = Platform()
        platform.adapters[adapter.id] = adapter
        dest_lat = sim.position.lat + 0.01
        try:
            await _drop_durables()
            assert await start_pipeline()
            assert await start_command_bridge(platform.adapters.get)
            seen = {event.event_id for event in event_store.get_by_asset(adapter.id)}
            cmd = Command(
                asset_id=adapter.id,
                command_type=CommandType.GOTO,
                issued_by="operator-1",
                parameters={"lat": dest_lat, "lon": sim.position.lon, "alt": 10},
            )
            result = await platform.issue(cmd)
            assert result.status == "sent", result.result
            assert sim.target is not None
            assert abs(sim.target.lat - dest_lat) < 1e-6
            fresh = [
                event for event in event_store.get_by_asset(adapter.id)
                if event.event_id not in seen
            ]
            assert any(
                event.payload.get("command_id") == cmd.id and event.payload.get("operator_id") == "operator-1"
                for event in fresh
            )
            assert any(event.payload.get("command_id") == cmd.id and event.payload.get("ack") is True for event in fresh)
            stored = _row(
                "SELECT data FROM events WHERE asset_id = :asset_id AND data->>'command_id' = :command_id",
                asset_id=adapter.id,
                command_id=cmd.id,
            )
            # At least the command Event is durable. The ack Event is selected below.
            rows = []
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                from src.data_services.postgres_backend import admin_engine

                with admin_engine().connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT data FROM events WHERE asset_id = :asset_id "
                            "AND data->>'command_id' = :command_id"
                        ),
                        {"asset_id": adapter.id, "command_id": cmd.id},
                    ).all()
                payloads = [_loads(row[0]) for row in rows]
                if any(item.get("ack") is True for item in payloads) and any(
                    item.get("operator_id") == "operator-1" for item in payloads
                ):
                    break
                await asyncio.sleep(0.2)
            payloads = [_loads(row[0]) for row in rows]
            assert any(item.get("operator_id") == "operator-1" for item in payloads), stored
            assert any(item.get("ack") is True for item in payloads)
        finally:
            await _shutdown()

    asyncio.run(body())


@pytest.mark.nats
def test_missing_ack_raises_an_alert(monkeypatch):
    _arm(monkeypatch)
    monkeypatch.setenv("CNC_ACK_TIMEOUT_S", "0.6")

    async def body():
        sim = create_demo_fleet()["ugv-02"]
        adapter = SimAssetAdapter(sim)
        platform = Platform()
        platform.adapters[adapter.id] = adapter
        try:
            await _drop_durables()
            cmd = Command(
                asset_id=adapter.id,
                command_type=CommandType.GOTO,
                issued_by="operator-1",
                parameters={"lat": sim.position.lat + 0.02, "lon": sim.position.lon, "alt": 5},
            )
            result = await platform.issue(cmd)
            assert result.status == "failed"
            assert result.result["error"] == "ack timeout"
            assert sim.target is None
            assert any(
                alert.title == "Command ack timeout" and alert.asset_id == adapter.id
                for alert in platform.store.alerts.values()
            )
            row = _row(
                "SELECT level, message FROM alerts WHERE asset_id = :asset_id AND message LIKE :pattern",
                asset_id=adapter.id,
                pattern="%No ack%",
            )
            assert row is not None
            assert row[0] == "warning"
            events = event_store.get_by_asset(adapter.id)
            assert any(event.payload.get("command_id") == cmd.id and not event.payload.get("ack") for event in events)
            assert not any(event.payload.get("ack") for event in events)
        finally:
            await _shutdown()

    asyncio.run(body())


@pytest.mark.nats
def test_invalid_mavlink_telemetry_lands_on_the_dlq(monkeypatch):
    _arm(monkeypatch)

    async def body():
        token = f"bad-{uuid4().hex[:8]}"
        try:
            await _drop_durables()
            adapter = MAVLinkAdapter(Asset(name="dlq", asset_type=AssetType.UAV))
            assert await adapter.publish_vendor_telemetry({"asset_id": token}) is None
            message = await get_bus().js.get_msg("CNC_DLQ", subject="cnc.dlq")
            body_json = json.loads(message.data)
            assert body_json["payload"]["asset_id"] == token
            assert "position" in body_json["reason"]
        finally:
            await _shutdown()

    asyncio.run(body())
