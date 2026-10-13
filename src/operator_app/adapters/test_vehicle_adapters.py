"""Adapter helpers that do not need SITL or a ROS graph."""

import pytest

from src.operator_app.adapters.mavlink_adapter import (
    ACK_ACCEPTED,
    ACK_IN_PROGRESS,
    MAVLinkAdapter,
    STREAM_HZ,
    ack_is_final,
    ack_ok,
    command_ack_budget,
)
from src.operator_app.adapters.ros2_adapter import (
    header_source_ms,
    latlon_from_local,
    local_offset_m,
    sim_stamp_to_wall_ms,
)
from src.operator_app.app import Asset, AssetType


def test_command_ack_accepts_only_a_final_accepted_result():
    assert ack_ok(ACK_ACCEPTED)
    assert not ack_is_final(ACK_IN_PROGRESS)
    assert ack_is_final(2)
    assert not ack_ok(2)
    assert command_ack_budget() >= 1.0


class _MavMsg:
    def __init__(self, kind, **fields):
        self.kind = kind
        self.__dict__.update(fields)

    def get_type(self):
        return self.kind


def test_ros_header_stamp_is_source_time_only_near_the_wall_clock():
    assert header_source_ms(1_700_000_000.0, 1_700_000_000.2) == pytest.approx(1_700_000_000_000.0)
    assert header_source_ms(100.0, 1_700_000_000.0) is None
    assert header_source_ms(None, 1.0) is None
    assert header_source_ms(0.0, 0.1) is None


def test_sim_stamp_keeps_one_offset_to_the_wall_clock():
    source, offset = sim_stamp_to_wall_ms(100.0, 1_700_000_000.0, None)
    assert offset == pytest.approx(1_700_000_000.0 - 100.0)
    assert source == pytest.approx(1_700_000_000_000.0)
    later, same = sim_stamp_to_wall_ms(100.4, 1_700_000_000.5, offset)
    assert same == pytest.approx(offset)
    # Sim advanced 0.4 s while the wall advanced 0.5 s, so the sample is 100 ms late.
    assert later == pytest.approx((100.4 + offset) * 1000.0)
    assert 1_700_000_000.5 * 1000.0 - later == pytest.approx(100.0)


def test_mavlink_position_gap_is_the_receive_interval(monkeypatch):
    assert STREAM_HZ == 10
    asset = Asset(id="uav-01", name="Eagle-1", asset_type=AssetType.UAV, lat=17.385, lon=78.4867, alt=30)
    adapter = MAVLinkAdapter(asset)
    clock = {"now": 1_000.0}
    monkeypatch.setattr("src.operator_app.adapters.mavlink_adapter.time.time", lambda: clock["now"])

    def position():
        return _MavMsg(
            "GLOBAL_POSITION_INT",
            lat=173_850_000,
            lon=784_867_000,
            relative_alt=30_000,
            hdg=9000,
            vx=0,
            vy=0,
        )

    adapter._on_message(position())
    clock["now"] = 1_000.2
    adapter._on_message(position())
    assert adapter._source_ms is None
    assert adapter._gap_ms == pytest.approx(200.0)
    adapter._running = True
    adapter._master = object()
    sample = adapter.poll_telemetry()
    assert sample is not None
    assert sample.extras["timing"]["source_ms"] is None
    assert sample.extras["timing"]["message_gap_ms"] == pytest.approx(200.0)
    assert sample.extras["timing"]["poll_ms"] > 0


def test_system_time_offsets_the_position_boot_clock(monkeypatch):
    asset = Asset(id="uav-01", name="Eagle-1", asset_type=AssetType.UAV, lat=17.385, lon=78.4867, alt=30)
    adapter = MAVLinkAdapter(asset)
    clock = {"now": 1_700_000_000.0}
    monkeypatch.setattr("src.operator_app.adapters.mavlink_adapter.time.time", lambda: clock["now"])
    adapter._on_message(_MavMsg("SYSTEM_TIME", time_unix_usec=1_700_000_000_000_000, time_boot_ms=10_000))
    adapter._on_message(_MavMsg(
        "GLOBAL_POSITION_INT",
        lat=173_850_000,
        lon=784_867_000,
        relative_alt=30_000,
        hdg=9000,
        vx=0,
        vy=0,
        time_boot_ms=10_250,
    ))
    # unix ms at boot is 1_700_000_000_000 - 10_000. The position is 250 ms later.
    assert adapter._source_ms == pytest.approx(1_700_000_000_250.0)
    assert adapter._adapter_ms == pytest.approx(1_700_000_000_000.0)


def test_heartbeat_keeps_the_command_outcome():
    asset = Asset(id="uav-01", name="Eagle-1", asset_type=AssetType.UAV, lat=17.385, lon=78.4867, alt=30)
    adapter = MAVLinkAdapter(asset)
    adapter._master = object()
    adapter._mission = "airborne"
    adapter._on_message(_MavMsg("HEARTBEAT", type=2, autopilot=3, base_mode=129, custom_mode=4))
    assert adapter._armed is True
    assert adapter._mode == "GUIDED"
    assert adapter._mission == "airborne"


def test_ros_offset_round_trips_a_short_move():
    home_lat, home_lon = 17.3820, 78.4840
    east, north = local_offset_m(17.3830, 78.4850, home_lat, home_lon)
    assert north > 100.0
    assert east > 100.0
    lat, lon = latlon_from_local(east, north, home_lat, home_lon)
    assert lat == pytest.approx(17.3830, abs=1e-6)
    assert lon == pytest.approx(78.4850, abs=1e-6)
