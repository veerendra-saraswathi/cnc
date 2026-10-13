"""Gazebo diff-drive. Skipped unless CNC_ROS2=1 and rclpy can see the waypoint action."""

import asyncio
import os

import pytest

pytestmark = pytest.mark.ros2


@pytest.mark.ros2
def test_ros2_odom_and_goto_ack():
    if os.environ.get("CNC_ROS2") != "1":
        pytest.skip("set CNC_ROS2=1 when the Gazebo waypoint action is up")
    try:
        import rclpy  # noqa: F401
    except ImportError:
        pytest.skip("rclpy is not installed in this interpreter")
    from src.operator_app.adapters.ros2_adapter import ROS2Adapter
    from src.operator_app.app import Asset, AssetStatus, AssetType, Command, CommandType

    namespace = os.environ.get("CNC_ROS2_NAMESPACE", "ugv_01")
    asset = Asset(
        id="ugv-01",
        name="Wolf-1",
        asset_type=AssetType.UGV,
        status=AssetStatus.OFFLINE,
        lat=17.3820,
        lon=78.4840,
        alt=0.0,
        home_lat=17.3820,
        home_lon=78.4840,
    )
    adapter = ROS2Adapter(asset, namespace=namespace)

    async def body():
        await adapter.start()
        assert adapter._have_odom, adapter._mission
        sample = adapter.poll_telemetry()
        assert sample is not None
        assert sample.asset_id == "ugv-01"
        goto = Command(
            asset_id=asset.id,
            command_type=CommandType.GOTO,
            parameters={"lat": 17.3823, "lon": 78.4842, "alt": 0},
            issued_by="operator:op-1",
        )
        assert await adapter.send_command(goto) is True
        assert adapter._mission == "navigating"
        hold = Command(
            asset_id=asset.id,
            command_type=CommandType.HOLD,
            issued_by="operator:op-1",
        )
        assert await adapter.send_command(hold) is True
        await adapter.stop()

    asyncio.run(body())
