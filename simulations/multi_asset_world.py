"""
Multi-Asset Digital Twin / SIL Foundation
"""

from __future__ import annotations
import math
from typing import Dict, List, Optional
from dataclasses import dataclass

from src.asset_integration.models import (
    AssetType, AssetStatus, Position, Telemetry, Command, CommandType
)


@dataclass
class SimAsset:
    asset_id: str
    asset_type: AssetType
    position: Position
    speed_mps: float
    target: Optional[Position] = None
    status: AssetStatus = AssetStatus.IDLE


class MultiAssetWorld:
    def __init__(self):
        self.assets: Dict[str, SimAsset] = {
            "UAV-01": SimAsset("UAV-01", AssetType.UAV, Position(lat=17.3850, lon=78.4867, alt=80), 14.0),
            "UAV-02": SimAsset("UAV-02", AssetType.UAV, Position(lat=17.3900, lon=78.4920, alt=90), 14.0),
            "UGV-01": SimAsset("UGV-01", AssetType.UGV, Position(lat=17.3800, lon=78.4800, alt=0), 5.5),
            "UGV-02": SimAsset("UGV-02", AssetType.UGV, Position(lat=17.3750, lon=78.4950, alt=0), 5.5),
        }

    def register_with_coordinator(self, coordinator):
        for a in self.assets.values():
            coordinator.register_asset(a.asset_id, a.asset_type, a.position)

    def apply_commands(self, commands: List[Command]):
        for cmd in commands:
            asset = self.assets.get(cmd.asset_id)
            if not asset or cmd.type != CommandType.GOTO:
                continue
            p = cmd.parameters
            asset.target = Position(lat=p["lat"], lon=p["lon"], alt=p.get("alt", 0))
            asset.status = AssetStatus.ACTIVE
            print(f"[World] {asset.asset_id} ← GOTO ({asset.target.lat:.5f}, {asset.target.lon:.5f})")

    def step(self, dt: float = 1.0):
        for asset in self.assets.values():
            if asset.target is None:
                continue

            dist = self._distance_m(asset.position, asset.target)

            # Snap when close → reliable completion
            if dist < 40.0:
                asset.position = asset.target
                asset.target = None
                asset.status = AssetStatus.IDLE
                continue

            step = min(asset.speed_mps * dt, dist)
            ratio = step / dist
            asset.position = Position(
                lat=asset.position.lat + (asset.target.lat - asset.position.lat) * ratio,
                lon=asset.position.lon + (asset.target.lon - asset.position.lon) * ratio,
                alt=asset.position.alt + (asset.target.alt - asset.position.alt) * ratio,
            )
            asset.status = AssetStatus.ACTIVE

    def _distance_m(self, p1: Position, p2: Position) -> float:
        R = 6371000
        lat1, lon1 = math.radians(p1.lat), math.radians(p1.lon)
        lat2, lon2 = math.radians(p2.lat), math.radians(p2.lon)
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
        return 2 * R * math.asin(math.sqrt(a))

    def get_telemetry(self) -> List[Telemetry]:
        return [
            Telemetry(
                asset_id=a.asset_id,
                position=a.position,
                speed=a.speed_mps if a.target else 0.0,
                status=a.status,
                mode="auto" if a.target else "idle",
            )
            for a in self.assets.values()
        ]
        