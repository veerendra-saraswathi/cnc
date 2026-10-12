"""
Lightweight simulated assets with position trail support.
"""

import time
import math
from datetime import datetime, timezone
from typing import Dict, List
from src.asset_integration.models import (
    AssetType, AssetStatus, Position, Telemetry, AssetInfo
)


class SimulatedAsset:
    def __init__(
        self,
        asset_id: str,
        name: str,
        asset_type: AssetType,
        start_lat: float = 17.3850,
        start_lon: float = 78.4867,
        start_alt: float = 50.0,
    ):
        self.info = AssetInfo(
            asset_id=asset_id,
            name=name,
            asset_type=asset_type,
            capabilities=["goto", "loiter", "rtl", "stop"],
            max_speed=15.0 if asset_type == AssetType.UAV else 5.0,
        )
        self.position = Position(lat=start_lat, lon=start_lon, alt=start_alt, heading=0.0)
        self.status = AssetStatus.IDLE
        self.speed = 0.0
        self.battery = 100.0
        self.target: Position | None = None
        self.trail: List[List[float]] = [[start_lat, start_lon]]   # [[lat, lon], ...]

    def update(self, dt: float = 1.0):
        if self.target and self.status in (AssetStatus.ACTIVE, AssetStatus.MISSION, AssetStatus.RETURNING):
            dlat = self.target.lat - self.position.lat
            dlon = self.target.lon - self.position.lon
            dist = math.sqrt(dlat**2 + dlon**2) * 111_000

            if dist < 5.0:
                self.position = self.target
                self.speed = 0.0
                self.status = AssetStatus.IDLE
                self.target = None
            else:
                max_speed = self.info.max_speed or 5.0
                step = min(max_speed * dt / 111_000, dist / 111_000)
                self.position.lat += dlat * (step / (dist / 111_000))
                self.position.lon += dlon * (step / (dist / 111_000))
                self.speed = max_speed
                self.position.heading = math.degrees(math.atan2(dlon, dlat)) % 360

                # Record trail (keep last 80 points)
                self.trail.append([self.position.lat, self.position.lon])
                if len(self.trail) > 80:
                    self.trail = self.trail[-80:]

        if self.speed > 0.1:
            self.battery = max(0.0, self.battery - 0.02 * dt)
        else:
            self.battery = max(0.0, self.battery - 0.005 * dt)

        if self.battery < 15.0 and self.status != AssetStatus.RETURNING:
            self.status = AssetStatus.RETURNING

    def get_telemetry(self) -> Telemetry:
        return Telemetry(
            asset_id=self.info.asset_id,
            timestamp=datetime.now(timezone.utc),
            position=self.position,
            speed=self.speed,
            battery_percent=round(self.battery, 1),
            status=self.status,
            mode="auto" if self.target else "idle",
        )

    def send_command(self, cmd_type: str, params: dict = None):
        params = params or {}
        if cmd_type == "goto":
            self.target = Position(
                lat=params.get("lat", self.position.lat),
                lon=params.get("lon", self.position.lon),
                alt=params.get("alt", self.position.alt),
            )
            self.status = AssetStatus.MISSION
        elif cmd_type == "stop":
            self.target = None
            self.speed = 0.0
            self.status = AssetStatus.IDLE
        elif cmd_type == "rtl":
            self.target = Position(lat=17.3850, lon=78.4867, alt=50.0)
            self.status = AssetStatus.RETURNING


def create_demo_fleet() -> Dict[str, SimulatedAsset]:
    return {
        "uav-01": SimulatedAsset("uav-01", "Eagle-1", AssetType.UAV, 17.3850, 78.4867, 80),
        "uav-02": SimulatedAsset("uav-02", "Eagle-2", AssetType.UAV, 17.3900, 78.4900, 90),
        "ugv-01": SimulatedAsset("ugv-01", "Wolf-1", AssetType.UGV, 17.3820, 78.4840, 0),
        "ugv-02": SimulatedAsset("ugv-02", "Wolf-2", AssetType.UGV, 17.3880, 78.4920, 0),
    }