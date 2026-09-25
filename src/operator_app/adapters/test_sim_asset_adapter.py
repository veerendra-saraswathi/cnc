"""Sim fleet adapter: four assets, motion, hold, own-home RTL, command Event."""

import asyncio
import time

from simulations.simple_assets import create_demo_fleet
from src.data_services.event_store import event_store
from src.operator_app.adapters.sim_asset_adapter import SimAssetAdapter
from src.operator_app.app import AssetStatus, Command, CommandType, Platform


def test_fleet_is_two_uav_and_two_ugv():
    adapters = [SimAssetAdapter(sim) for sim in create_demo_fleet().values()]
    assert len(adapters) == 4
    kinds = sorted(a.asset.asset_type.value for a in adapters)
    assert kinds == ["uav", "uav", "ugv", "ugv"]
    assert sorted(a.asset.name for a in adapters) == [
        "Eagle-1", "Eagle-2", "Wolf-1", "Wolf-2",
    ]


def test_start_uses_sim_position_not_recovered_mock_fix():
    sim = create_demo_fleet()["uav-01"]
    adapter = SimAssetAdapter(sim)
    adapter.asset.lat = 17.45
    adapter.asset.lon = 78.48
    adapter.asset.alt = 0.0
    asyncio.run(adapter.start())
    assert abs(adapter.asset.lat - 17.3850) < 1e-6
    assert abs(adapter.asset.lon - 78.4867) < 1e-6
    assert abs(adapter.asset.alt - 80.0) < 1e-6


def test_goto_then_hold():
    async def body():
        sim = create_demo_fleet()["uav-01"]
        adapter = SimAssetAdapter(sim)
        await adapter.start()
        origin = sim.position.lat
        ok = await adapter.send_command(Command(
            asset_id=adapter.id,
            command_type=CommandType.GOTO,
            parameters={"lat": origin + 0.005, "lon": sim.position.lon, "alt": 100},
        ))
        assert ok
        adapter._last_tick = time.monotonic() - 30
        telem = await adapter.get_telemetry()
        assert telem is not None
        assert telem.status == AssetStatus.MISSION
        assert telem.lat > origin + 0.001

        ok = await adapter.send_command(Command(
            asset_id=adapter.id,
            command_type=CommandType.HOLD,
        ))
        assert ok
        assert sim.target is None
        held = await adapter.get_telemetry()
        assert held is not None
        assert held.status == AssetStatus.IDLE

    asyncio.run(body())


def test_rtl_uses_own_start_not_eagle_home():
    async def body():
        sim = create_demo_fleet()["ugv-02"]
        adapter = SimAssetAdapter(sim)
        await adapter.start()
        # Walk it off its start so RTL has somewhere to aim.
        await adapter.send_command(Command(
            asset_id=adapter.id,
            command_type=CommandType.GOTO,
            parameters={"lat": 17.3800, "lon": 78.4800, "alt": 0},
        ))
        ok = await adapter.send_command(Command(
            asset_id=adapter.id,
            command_type=CommandType.RTL,
        ))
        assert ok
        assert sim.target is not None
        assert abs(sim.target.lat - 17.3880) < 1e-6
        assert abs(sim.target.lon - 78.4920) < 1e-6
        assert abs(sim.target.lat - 17.3850) > 1e-3

    asyncio.run(body())


def test_issue_records_operator_event():
    async def body():
        sim = create_demo_fleet()["ugv-01"]
        adapter = SimAssetAdapter(sim)
        platform = Platform()
        platform.adapters[adapter.id] = adapter
        seen = {e.event_id for e in event_store.get_by_asset(adapter.id)}
        cmd = Command(
            asset_id=adapter.id,
            command_type=CommandType.HOLD,
            issued_by="operator-1",
        )
        result = await platform.issue(cmd)
        assert result.status == "sent"
        fresh = [e for e in event_store.get_by_asset(adapter.id) if e.event_id not in seen]
        assert len(fresh) == 1
        assert fresh[0].asset_id == adapter.id
        assert fresh[0].data["operator_id"] == "operator-1"
        assert fresh[0].timestamp is not None

    asyncio.run(body())
