"""
Hardware-agnostic Asset Adapter interface.

Any real vehicle (MAVLink, ROS 2, proprietary SDK) or the existing
MockAsset implements this interface. The rest of the platform only
talks to AssetAdapter instances.
"""

from __future__ import annotations

import abc
from typing import Optional

from ..app import Asset, Command, Telemetry   # temporary relative import


class AssetAdapter(abc.ABC):
    """Abstract base for every connected autonomous asset."""

    def __init__(self, asset: Asset) -> None:
        self.asset = asset
        self._running = False

    @property
    def asset_id(self) -> str:
        return self.asset.id

    @abc.abstractmethod
    async def connect(self) -> None:
        """Establish link / start simulation loop."""

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Clean shutdown."""

    @abc.abstractmethod
    async def send_command(self, command: Command) -> bool:
        """Dispatch a command. Return True if accepted by the asset."""

    @abc.abstractmethod
    async def get_telemetry(self) -> Optional[Telemetry]:
        """Latest telemetry sample (or None if offline)."""

    async def publish_vendor_telemetry(self, raw):
        """Validate kernel Telemetry and publish it. Invalid samples go to cnc.dlq."""
        from src.operator_app.adapters.telemetry_publish import accept_vendor_telemetry
        return await accept_vendor_telemetry(raw)

    async def start(self) -> None:
        self._running = True
        await self.connect()

    async def stop(self) -> None:
        self._running = False
        await self.disconnect()
