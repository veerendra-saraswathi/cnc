"""Wrap one SimulatedAsset so the operator platform can poll and task it."""

from __future__ import annotations

import time

from simulations.simple_assets import SimulatedAsset
from src.asset_integration.models import AssetStatus as SimStatus
from src.operator_app.adapters.telemetry_publish import accept_vendor_telemetry
from src.operator_app.app import (
    Asset,
    AssetStatus,
    AssetType,
    Command,
    CommandType,
    Telemetry,
    utcnow,
)

# Operator AssetStatus has no "active" or "error".
_STATUS = {
    "offline": AssetStatus.OFFLINE,
    "idle": AssetStatus.IDLE,
    "active": AssetStatus.MISSION,
    "mission": AssetStatus.MISSION,
    "returning": AssetStatus.RETURNING,
    "emergency": AssetStatus.EMERGENCY,
    "error": AssetStatus.EMERGENCY,
}


class SimAssetAdapter:
    """Duck-types the object Platform.register already drives."""

    def __init__(self, sim: SimulatedAsset) -> None:
        self.sim = sim
        self._home_lat = sim.position.lat
        self._home_lon = sim.position.lon
        self._home_alt = sim.position.alt
        self._running = False
        self._last_tick = time.monotonic()

        caps = list(sim.info.capabilities)
        if sim.info.asset_type.value == "uav":
            for name in ("takeoff", "land", "hold"):
                if name not in caps:
                    caps.append(name)
        elif "hold" not in caps:
            caps.append("hold")

        self.asset = Asset(
            name=sim.info.name,
            asset_type=AssetType(sim.info.asset_type.value),
            status=AssetStatus.OFFLINE,
            lat=self._home_lat,
            lon=self._home_lon,
            alt=self._home_alt,
            heading=sim.position.heading,
            speed=0.0,
            battery=sim.battery,
            capabilities=caps,
            home_lat=self._home_lat,
            home_lon=self._home_lon,
        )

    @property
    def id(self) -> str:
        return self.asset.id

    async def start(self) -> None:
        self._running = True
        self._last_tick = time.monotonic()
        # register() may have copied a recovered mock fix onto this asset.
        self._apply_sim_pose()
        self.asset.status = AssetStatus.IDLE
        self.asset.last_seen = utcnow()

    async def stop(self) -> None:
        self._running = False
        self.asset.status = AssetStatus.OFFLINE

    def _apply_sim_pose(self) -> None:
        pos = self.sim.position
        self.asset.lat = pos.lat
        self.asset.lon = pos.lon
        self.asset.alt = pos.alt
        self.asset.heading = pos.heading
        self.asset.speed = self.sim.speed
        self.asset.battery = self.sim.battery

    async def publish_vendor_telemetry(self, raw):
        return await accept_vendor_telemetry(raw)

    async def send_command(self, cmd: Command) -> bool:
        from src.operator_app.adapters.idempotency import deliver_once
        return await deliver_once(self._apply_command, cmd)

    async def _apply_command(self, cmd: Command) -> bool:
        kind = cmd.command_type
        params = cmd.parameters or {}
        if kind == CommandType.GOTO:
            try:
                lat = float(params["lat"])
                lon = float(params["lon"])
                alt = float(params.get("alt", self.sim.position.alt))
            except (KeyError, TypeError, ValueError):
                return False
            self.sim.send_command("goto", {"lat": lat, "lon": lon, "alt": alt})
            return True
        if kind == CommandType.HOLD:
            self.sim.send_command("stop")
            return True
        if kind == CommandType.RTL:
            # SimulatedAsset.rtl always returns to Eagle-1's start.
            self.sim.send_command("goto", {
                "lat": self._home_lat,
                "lon": self._home_lon,
                "alt": self._home_alt,
            })
            self.sim.status = SimStatus.RETURNING
            return True
        if kind == CommandType.TAKEOFF:
            if self.asset.asset_type != AssetType.UAV:
                return False
            try:
                alt = float(params.get("altitude", params.get("alt", 50)))
            except (TypeError, ValueError):
                return False
            self.sim.send_command("goto", {
                "lat": self.sim.position.lat,
                "lon": self.sim.position.lon,
                "alt": alt,
            })
            return True
        if kind == CommandType.LAND:
            if self.asset.asset_type != AssetType.UAV:
                return False
            self.sim.send_command("goto", {
                "lat": self.sim.position.lat,
                "lon": self.sim.position.lon,
                "alt": 0.0,
            })
            return True
        return False

    def poll_telemetry(self) -> Telemetry | None:
        """Sample and stamp. Does not publish, so the caller can push a frame first."""
        if not self._running:
            return None
        now = time.monotonic()
        dt = max(0.0, now - self._last_tick)
        self._last_tick = now
        self.sim.update(dt)
        sample = self.sim.get_telemetry()
        status = _STATUS.get(
            sample.status.value if hasattr(sample.status, "value") else str(sample.status),
            AssetStatus.IDLE,
        )
        seen = utcnow()
        poll_ms = seen.timestamp() * 1000.0
        self.asset.lat = sample.position.lat
        self.asset.lon = sample.position.lon
        self.asset.alt = sample.position.alt
        self.asset.heading = sample.position.heading
        self.asset.speed = sample.speed
        self.asset.battery = sample.battery_percent
        self.asset.status = status
        self.asset.last_seen = seen
        # The in-process sim has no separate publish clock. adapter_ms is the poll.
        return Telemetry(
            asset_id=self.asset.id,
            lat=sample.position.lat,
            lon=sample.position.lon,
            alt=sample.position.alt,
            heading=sample.position.heading,
            speed=sample.speed,
            battery=sample.battery_percent,
            status=status,
            timestamp=seen,
            extras={
                "timing": {
                    "source_ms": None,
                    "adapter_ms": poll_ms,
                    "poll_ms": poll_ms,
                    "message_gap_ms": None,
                },
            },
        )

    async def get_telemetry(self) -> Telemetry | None:
        sample = self.poll_telemetry()
        if sample is None:
            return None
        return await self.publish_vendor_telemetry(sample)
