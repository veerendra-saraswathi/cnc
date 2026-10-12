"""
ROS 2 Adapter skeleton.

Stub for future integration with ROS 2 based vehicles / simulators
(Gazebo, Isaac, custom).
"""

from __future__ import annotations

from typing import Optional

from .base import AssetAdapter
from ..app import Asset, Command, Telemetry, AssetStatus


class ROS2Adapter(AssetAdapter):
    """
    Skeleton for a ROS 2 vehicle.

    Later this will subscribe to /odom, /battery_state, etc.
    and publish to /cmd_vel or navigation action servers.
    """

    def __init__(self, asset: Asset, namespace: str = ""):
        super().__init__(asset)
        self.namespace = namespace

    async def connect(self) -> None:
        # TODO: create rclpy node, subscribers, publishers
        self.asset.status = AssetStatus.IDLE
        print(f"[ROS2Adapter] {self.asset.name} would start ROS 2 node (ns={self.namespace})")

    async def disconnect(self) -> None:
        self.asset.status = AssetStatus.OFFLINE
        print(f"[ROS2Adapter] {self.asset.name} stopped")

    async def send_command(self, command: Command) -> bool:
        # TODO: publish to the appropriate ROS 2 topics / actions
        from src.operator_app.adapters.idempotency import deliver_once

        async def apply(cmd: Command) -> tuple[bool, str]:
            print(f"[ROS2Adapter] {self.asset.name} received {cmd.command_type}")
            return False, "not implemented"

        return await deliver_once(apply, command)

    async def get_telemetry(self) -> Optional[Telemetry]:
        # TODO: read latest messages from subscriptions
        return None
