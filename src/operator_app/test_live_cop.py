"""Live COP push, demo fleet, zone breach, keep-out rejection, nearest UGV."""

from __future__ import annotations

import asyncio
import time

import pytest

from src.data.bus import get_bus
from src.data.subjects import telemetry_subject
from src.data_services.event_store import event_store
from src.data_services.repositories import get_repositories
from src.kernel.models import (
    Alert,
    AlertLevel,
    Asset,
    AssetStatus,
    AssetType,
    Command,
    CommandType,
    Detection,
    Zone,
)
from src.operator_app.app import platform, ws_cop
from src.operator_app.coordinate import nearest_ugv, task_nearest_ugv, track_from_detection
from src.operator_app.demo_fleet import demo_adapters, demo_homes, demo_subjects
from src.operator_app.live_feed import (
    command_phase,
    hub,
    mission_progress,
    notify_live,
    present_snapshot,
)
from src.operator_app.zones import apply_zone_check, keep_out_zone, point_in_polygon

FAR = [
    {"lat": 1.0, "lon": 1.0},
    {"lat": 1.0, "lon": 1.2},
    {"lat": 1.2, "lon": 1.2},
    {"lat": 1.2, "lon": 1.0},
]


class _Probe:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_command(self, cmd) -> bool:
        self.sent.append(cmd.id)
        return True


class _MemStore:
    def __init__(self) -> None:
        self.alerts: dict[str, Alert] = {}
        self.added: list[str] = []

    async def add_alert(self, alert: Alert) -> Alert:
        self.alerts[alert.id] = alert
        self.added.append(alert.id)
        return alert


class _Socket:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def accept(self) -> None:
        return None

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        return None


def _square(zone_id: str, kind: str, lat0: float, lon0: float, span: float = 0.2) -> Zone:
    return Zone(
        id=zone_id,
        name=zone_id,
        kind=kind,
        polygon=[
            {"lat": lat0, "lon": lon0},
            {"lat": lat0, "lon": lon0 + span},
            {"lat": lat0 + span, "lon": lon0 + span},
            {"lat": lat0 + span, "lon": lon0},
        ],
    )


def test_point_in_polygon_matches_the_square():
    assert point_in_polygon(1.1, 1.1, FAR)
    assert not point_in_polygon(17.4, 78.5, FAR)


def test_command_phase_and_mission_progress():
    assert command_phase("pending", {"queued": True}) == "pending"
    assert command_phase("sent", None) == "sent"
    assert command_phase("sent", {"ack": True}) == "acked"
    assert command_phase("failed", {"error": "ack timeout"}) == "timeout"
    assert command_phase("failed", {"error": "keep-out"}) == "failed"
    mission = {"status": "active", "assigned_assets": ["a", "b"]}
    commands = [
        {"asset_id": "a", "phase": "acked"},
        {"asset_id": "b", "phase": "pending"},
    ]
    assert mission_progress(mission, commands) == 0.5
    assert mission_progress({"status": "completed", "assigned_assets": []}, []) == 1.0
    frame = present_snapshot(
        {
            "timestamp": "2026-10-12T00:00:00+00:00",
            "assets": [{"id": "uav-01", "name": "Eagle-1", "asset_type": "uav", "lat": 1, "lon": 2}],
            "tracks": [],
            "alerts": [],
            "missions": [{"id": "m1", "name": "Recon", "status": "active", "assigned_assets": ["uav-01"]}],
            "recent_commands": [{
                "id": "c1",
                "asset_id": "uav-01",
                "command_type": "goto",
                "status": "sent",
                "result": {"ack": True},
            }],
        },
        {
            "platform": [{
                "asset_id": "uav-01",
                "position": {"lat": 17.5, "lon": 78.5, "alt": 40, "heading": 10},
                "speed": 4,
                "battery_percent": 80,
                "status": "mission",
            }],
        },
        zones=[{"id": "z", "name": "box", "kind": "keep-out", "ring": [[1, 1]]}],
    )
    assert frame["assets"][0]["lat"] == 17.5
    assert frame["assets"][0]["name"] == "Eagle-1"
    assert frame["recent_commands"][0]["phase"] == "acked"
    assert frame["missions"][0]["progress"] == 1.0
    assert frame["zones"][0]["kind"] == "keep-out"


def test_hub_pushes_a_frame_without_waiting():
    async def body():
        queue = hub.subscribe()
        try:
            hub.publish({"assets": [{"id": "pushed"}]})
            frame = await asyncio.wait_for(queue.get(), 0.2)
            assert frame["assets"][0]["id"] == "pushed"
        finally:
            hub.unsubscribe(queue)

    asyncio.run(body())


def test_websocket_pushes_the_next_frame(monkeypatch):
    monkeypatch.setattr("src.operator_app.live_feed.read_redis_overlay", lambda: {})
    monkeypatch.setattr("src.operator_app.live_feed.zone_rings", lambda: [])
    asset_id = "ws-push-asset"
    asset = Asset(
        id=asset_id,
        name="Push",
        asset_type=AssetType.UGV,
        status=AssetStatus.IDLE,
        lat=17.2,
        lon=78.2,
    )

    async def body():
        socket = _Socket()
        task = asyncio.create_task(ws_cop(socket))
        try:
            deadline = time.monotonic() + 0.4
            while time.monotonic() < deadline and not socket.sent:
                await asyncio.sleep(0.01)
            assert socket.sent, "initial frame was not pushed"
            platform.store.assets[asset_id] = asset
            notify_live()
            deadline = time.monotonic() + 0.3
            while time.monotonic() < deadline and len(socket.sent) < 2:
                await asyncio.sleep(0.01)
            assert len(socket.sent) >= 2
            assert any(item["id"] == asset_id for item in socket.sent[-1]["assets"])
        finally:
            platform.store.assets.pop(asset_id, None)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(body())


def test_demo_fleet_ids_homes_and_bus_subjects(monkeypatch):
    adapters = demo_adapters()
    assert [adapter.id for adapter in adapters] == ["uav-01", "uav-02", "ugv-01", "ugv-02"]
    kinds = [adapter.asset.asset_type for adapter in adapters]
    assert kinds.count(AssetType.UAV) == 2
    assert kinds.count(AssetType.UGV) == 2
    homes = demo_homes(adapters)
    assert len(set(homes.values())) == 4
    assert homes["uav-01"][0] == pytest.approx(17.3850)
    assert homes["ugv-02"][:2] == pytest.approx((17.3880, 78.4920))
    assert demo_subjects(adapters) == [
        "cnc.telemetry.uav-01",
        "cnc.telemetry.uav-02",
        "cnc.telemetry.ugv-01",
        "cnc.telemetry.ugv-02",
    ]
    subjects: list[str] = []

    async def capture(subject, payload, msg_id=None, timeout=None):
        subjects.append(subject)
        return True

    async def body():
        bus = get_bus()
        monkeypatch.setattr(bus, "publish_jetstream", capture)
        for adapter in adapters:
            await adapter.start()
            sample = await adapter.get_telemetry()
            assert sample is not None
            assert sample.asset_id == adapter.id
            await adapter.stop()

    asyncio.run(body())
    assert subjects == [telemetry_subject(adapter.id) for adapter in adapters]


def test_zone_breach_raises_one_alert_per_asset():
    zones = get_repositories().zones
    keep_out = _square("m4-keep-out", "keep-out", 1.0, 1.0)
    keep_in = _square("m4-keep-in", "keep-in", 2.0, 2.0)
    aoi = _square("m4-aoi", "aoi", 4.0, 4.0)
    store = _MemStore()
    inside = Asset(id="breach-in", name="Inside", asset_type=AssetType.UGV, lat=1.1, lon=1.1)
    outside = Asset(id="breach-out", name="Outside", asset_type=AssetType.UAV, lat=3.0, lon=3.0)
    safe = Asset(id="breach-safe", name="Safe", asset_type=AssetType.UGV, lat=2.1, lon=2.1)
    aoi_asset = Asset(id="breach-aoi", name="Aoi", asset_type=AssetType.UGV, lat=4.1, lon=4.1)
    try:
        zones.add(aoi)

        async def body():
            watched = await apply_zone_check(store, aoi_asset)
            assert watched is None
            zones.add(keep_out)
            first = await apply_zone_check(store, inside)
            second = await apply_zone_check(store, inside)
            assert first is not None
            assert first.id == f"zone-{keep_out.id}-{inside.id}"
            assert first.level == AlertLevel.CRITICAL
            assert second.id == first.id
            assert store.added == [first.id]
            zones.add(keep_in)
            held = await apply_zone_check(store, safe)
            assert held is None
            left = await apply_zone_check(store, outside)
            assert left is not None
            assert left.level == AlertLevel.WARNING
            assert left.id == f"zone-{keep_in.id}-{outside.id}"

        asyncio.run(body())
        assert keep_out_zone(1.1, 1.1).id == keep_out.id
        assert keep_out_zone(17.4, 78.5) is None
    finally:
        zones.remove(keep_out.id)
        zones.remove(keep_in.id)
        zones.remove(aoi.id)


def test_keep_out_goto_is_rejected_with_an_event():
    zones = get_repositories().zones
    zone = Zone(id="m4-reject-keep-out", name="far", kind="keep-out", polygon=FAR)
    asset_id = "keepout-reject-asset"
    probe = _Probe()
    platform.adapters[asset_id] = probe
    zones.add(zone)
    try:
        seen = {event.event_id for event in event_store.get_by_asset(asset_id)}

        async def body():
            rejected = await platform.issue(Command(
                asset_id=asset_id,
                command_type=CommandType.GOTO,
                parameters={"lat": 1.1, "lon": 1.1, "alt": 10},
                issued_by="operator-1",
            ))
            allowed = await platform.issue(Command(
                asset_id=asset_id,
                command_type=CommandType.GOTO,
                parameters={"lat": 17.4, "lon": 78.5, "alt": 40},
                issued_by="operator-1",
            ))
            return rejected, allowed

        rejected, allowed = asyncio.run(body())
        assert rejected.status == "failed"
        assert rejected.result["error"] == "keep-out"
        assert rejected.result["zone_id"] == zone.id
        assert allowed.status == "sent"
        assert probe.sent == [allowed.id]
        fresh = [event for event in event_store.get_by_asset(asset_id) if event.event_id not in seen]
        assert any(
            event.payload.get("command_id") == rejected.id and event.payload.get("reason") == "keep-out"
            for event in fresh
        )
        from src.data.outbox import command_published
        assert command_published(rejected.id) is False
    finally:
        zones.remove(zone.id)
        platform.adapters.pop(asset_id, None)


def test_detection_tasks_the_nearest_ugv():
    detection = Detection(asset_id="uav-detect", lat=17.381, lon=78.485, confidence=0.9)
    track = track_from_detection(detection)
    assert track.id == f"det-{detection.detection_id}"
    near = Asset(
        id="ugv-near-m4",
        name="Near",
        asset_type=AssetType.UGV,
        status=AssetStatus.IDLE,
        lat=17.3812,
        lon=78.4852,
    )
    far = Asset(
        id="ugv-far-m4",
        name="Far",
        asset_type=AssetType.UGV,
        status=AssetStatus.IDLE,
        lat=18.2,
        lon=79.2,
    )
    uav = Asset(
        id="uav-detect",
        name="Eye",
        asset_type=AssetType.UAV,
        status=AssetStatus.IDLE,
        lat=17.39,
        lon=78.49,
    )
    assert nearest_ugv([uav, far, near], track.lat, track.lon).id == near.id
    near_probe = _Probe()
    far_probe = _Probe()
    platform.adapters[near.id] = near_probe
    platform.adapters[far.id] = far_probe
    platform.store.assets[near.id] = near
    platform.store.assets[far.id] = far
    platform.store.assets[uav.id] = uav
    try:
        platform.store.tracks[track.id] = track
        issued = asyncio.run(task_nearest_ugv(platform, track.id))
        assert issued.asset_id == near.id
        assert issued.command_type == CommandType.GOTO
        assert issued.status == "sent"
        assert issued.parameters["lat"] == pytest.approx(track.lat)
        assert near_probe.sent == [issued.id]
        assert far_probe.sent == []
        events = event_store.get_by_asset(near.id)
        assert any(event.payload.get("command_id") == issued.id for event in events)
        actions = [action for action in platform.operator_actions if action.command_id == issued.id]
        assert len(actions) == 1
        assert actions[0].action == "task-nearest-ugv"
        assert actions[0].actor == issued.issued_by
        assert actions[0].asset_id == near.id
        assert actions[0].payload["track_id"] == track.id
    finally:
        platform.adapters.pop(near.id, None)
        platform.adapters.pop(far.id, None)
        platform.store.assets.pop(near.id, None)
        platform.store.assets.pop(far.id, None)
        platform.store.assets.pop(uav.id, None)
        platform.store.tracks.pop(track.id, None)


@pytest.mark.postgres
def test_postgis_keep_out_contains_the_interior_point(request):
    from src.data_services.test_repositories import _require_postgres

    _require_postgres(request)
    from src.data_services.postgres_backend import PostgresRepositories

    repos = PostgresRepositories()
    zone = Zone(id="m4-postgis-keep-out", name="far", kind="keep-out", polygon=FAR)
    repos.zones.remove(zone.id)
    try:
        repos.zones.add(zone)
        inside = {item.id for item in repos.zones.containing(1.1, 1.1)}
        outside = {item.id for item in repos.zones.containing(17.4, 78.5)}
        assert zone.id in inside
        assert zone.id not in outside
    finally:
        repos.zones.remove(zone.id)
