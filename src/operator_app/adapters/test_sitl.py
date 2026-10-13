"""ArduCopter SITL. Skipped unless CNC_SITL=1 and a vehicle is already sending UDP."""

import asyncio
import os

import pytest

pytestmark = pytest.mark.sitl


@pytest.mark.sitl
def test_sitl_goto_hold_and_rtl_ack():
    if os.environ.get("CNC_SITL") != "1":
        pytest.skip("set CNC_SITL=1 when ArduCopter SITL is sending MAVLink UDP")
    from src.operator_app.adapters.mavlink_adapter import MAVLinkAdapter
    from src.operator_app.app import Asset, AssetStatus, AssetType, Command, CommandType

    endpoint = os.environ.get("CNC_SITL_ENDPOINT", "udpin:0.0.0.0:14550")
    asset = Asset(
        id="uav-01",
        name="Eagle-1",
        asset_type=AssetType.UAV,
        status=AssetStatus.OFFLINE,
        lat=17.3850,
        lon=78.4867,
        alt=30.0,
        home_lat=17.3850,
        home_lon=78.4867,
    )
    adapter = MAVLinkAdapter(asset, connection_string=endpoint, home_alt=30.0)

    async def body():
        await adapter.start()
        assert adapter._master is not None, adapter._mission
        sample = adapter.poll_telemetry()
        assert sample is not None
        assert sample.asset_id == "uav-01"
        goto = Command(
            asset_id=asset.id,
            command_type=CommandType.GOTO,
            parameters={"lat": 17.3860, "lon": 78.4870, "alt": 30},
            issued_by="operator:op-1",
        )
        assert await adapter.send_command(goto) is True
        hold = Command(
            asset_id=asset.id,
            command_type=CommandType.HOLD,
            issued_by="operator:op-1",
        )
        assert await adapter.send_command(hold) is True
        rtl = Command(
            asset_id=asset.id,
            command_type=CommandType.RTL,
            issued_by="operator:op-1",
        )
        assert await adapter.send_command(rtl) is True
        await adapter.stop()

    asyncio.run(body())
