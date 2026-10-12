"""
MAVLink Adapter skeleton.

This is intentionally a stub. When you are ready to connect real
vehicles, implement the connect / send_command / get_telemetry
methods using pymavlink or mavsdk.
"""

from __future__ import annotations

from typing import Optional

from .base import AssetAdapter
from ..app import Asset, Command, Telemetry, AssetStatus


class MAVLinkAdapter(AssetAdapter):
    """
    Skeleton for a real MAVLink vehicle.

    Usage later:
        adapter = MAVLinkAdapter(asset, connection_string="udp:127.0.0.1:14550")
        await platform.register_adapter(adapter)
    """

    def __init__(self, asset: Asset, connection_string: str = "udp:127.0.0.1:14550"):
        super().__init__(asset)
        self.connection_string = connection_string
        self._master = None          # will hold the mavlink connection

    async def connect(self) -> None:
        # TODO: real implementation
        # from pymavlink import mavutil
        # self._master = mavutil.mavlink_connection(self.connection_string)
        # self._master.wait_heartbeat()
        self.asset.status = AssetStatus.IDLE
        print(f"[MAVLinkAdapter] {self.asset.name} would connect to {self.connection_string}")

    async def disconnect(self) -> None:
        # TODO: close connection cleanly
        self.asset.status = AssetStatus.OFFLINE
        print(f"[MAVLinkAdapter] {self.asset.name} disconnected")

    async def send_command(self, command: Command) -> bool:
        # TODO: translate Command → MAVLink message
        # (MISSION_ITEM, SET_MODE, COMMAND_LONG, etc.)
        from src.operator_app.adapters.idempotency import deliver_once

        async def apply(cmd: Command) -> tuple[bool, str]:
            print(f"[MAVLinkAdapter] {self.asset.name} received {cmd.command_type}")
            return False, "not implemented"

        return await deliver_once(apply, command)

    async def get_telemetry(self) -> Optional[Telemetry]:
        # TODO: read GLOBAL_POSITION_INT, HEARTBEAT, SYS_STATUS, etc.
        return None
