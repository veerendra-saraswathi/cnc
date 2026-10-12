"""Both historical constructors still build the kernel models."""

from src.kernel.models import (
    Asset,
    AssetStatus,
    AssetType,
    Command,
    CommandType,
    Position,
    Telemetry,
    Track,
    TrackState,
    TrackStatus,
)
from src.asset_integration.models import AssetInfo


def test_command_accepts_both_field_names():
    integration = Command(
        command_id="cmd-1",
        asset_id="uav-01",
        type=CommandType.GOTO,
        parameters={"lat": 1.0},
        issued_by="operator:op-1",
    )
    assert integration.id == "cmd-1"
    assert integration.command_id == "cmd-1"
    assert integration.type == CommandType.GOTO
    assert integration.command_type == CommandType.GOTO
    assert integration.actor == "operator:op-1"
    assert integration.payload == {"lat": 1.0}
    dumped = integration.model_dump(mode="json")
    assert dumped["id"] == "cmd-1"
    assert dumped["command_type"] == "goto"
    assert dumped["issued_by"] == "operator:op-1"
    assert "type" not in dumped
    assert "actor" not in dumped
    assert "payload" not in dumped

    operator = Command(
        asset_id="ugv-01",
        command_type=CommandType.HOLD,
        issued_by="operator-1",
        status="sent",
    )
    assert operator.status == "sent"
    assert operator.command_type == CommandType.HOLD


def test_telemetry_accepts_flat_and_nested_pose():
    nested = Telemetry(
        asset_id="uav-01",
        position=Position(lat=17.3, lon=78.4, alt=80, heading=10),
        battery_percent=88.0,
        status=AssetStatus.ACTIVE,
    )
    assert nested.lat == 17.3
    assert nested.battery == 88.0
    dumped = nested.model_dump(mode="json")
    assert dumped["position"]["lat"] == 17.3
    assert dumped["battery_percent"] == 88.0
    assert "lat" not in dumped

    flat = Telemetry(
        asset_id="ugv-01",
        lat=17.2,
        lon=78.5,
        alt=0,
        heading=5,
        battery=70,
        status=AssetStatus.IDLE,
    )
    assert flat.position.lon == 78.5
    assert flat.battery_percent == 70
    assert flat.heading == 5


def test_asset_info_alias_and_operator_pose():
    info = AssetInfo(
        asset_id="uav-01",
        name="Eagle-1",
        asset_type=AssetType.UAV,
        capabilities=["goto"],
        max_speed=15.0,
        model="quad",
    )
    assert info.asset_id == "uav-01"
    assert info.id == "uav-01"
    assert info.max_speed == 15.0
    assert info.model == "quad"
    assert "max_speed" not in info.model_dump()

    live = Asset(name="Eagle-1", asset_type=AssetType.UAV, lat=17.0, lon=78.0)
    body = live.model_dump()
    assert body["lat"] == 17.0
    assert "manufacturer" not in body
    assert "max_speed" not in body


def test_track_dump_keeps_operator_state_not_fusion_status():
    track = Track(label="Eagle-1", lat=17.0, lon=78.0, state=TrackState.ACTIVE)
    body = track.model_dump(mode="json")
    assert body["state"] == "active"
    assert "status" not in body
    assert "positions" not in body
    assert track.status == TrackStatus.TENTATIVE
    assert track.track_id == track.id
