"""Load fleet.yaml and build one adapter per asset.

The default file is the in-process sim. CNC_FLEET selects another file so a
SITL copter and a Gazebo rover can share the map with a sim vehicle.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_ADAPTERS = {"sim", "mavlink", "ros2"}
_TYPES = {"uav", "ugv"}


@dataclass(frozen=True)
class FleetSpec:
    id: str
    name: str
    type: str
    adapter: str
    endpoint: str
    home_lat: float
    home_lon: float
    home_alt: float


def fleet_path() -> Path:
    raw = os.environ.get("CNC_FLEET", "").strip()
    if raw:
        return Path(raw)
    return ROOT / "fleet.yaml"


def load_specs(path: Path | None = None) -> list[FleetSpec]:
    import yaml

    chosen = path or fleet_path()
    data = yaml.safe_load(chosen.read_text(encoding="utf-8")) or {}
    rows = data.get("assets")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{chosen} has no assets")
    specs: list[FleetSpec] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"{chosen} has a non-object asset")
        asset_id = str(row.get("id") or "").strip()
        name = str(row.get("name") or "").strip()
        asset_type = str(row.get("type") or "").strip().lower()
        adapter = str(row.get("adapter") or "").strip().lower()
        endpoint = str(row.get("endpoint") or "")
        home = row.get("home") or {}
        if asset_id in seen:
            raise ValueError(f"duplicate fleet id {asset_id}")
        if not asset_id or not name:
            raise ValueError(f"{chosen} asset is missing id or name")
        if asset_type not in _TYPES:
            raise ValueError(f"{asset_id} type must be uav or ugv")
        if adapter not in _ADAPTERS:
            raise ValueError(f"{asset_id} adapter must be sim, mavlink, or ros2")
        try:
            lat = float(home["lat"])
            lon = float(home["lon"])
            alt = float(home.get("alt", 0.0))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{asset_id} home needs lat and lon") from exc
        seen.add(asset_id)
        specs.append(FleetSpec(asset_id, name, asset_type, adapter, endpoint, lat, lon, alt))
    return specs


def load_fleet_adapters(path: Path | None = None) -> list:
    """Adapters in file order. Sim stays in-process. MAVLink and ROS connect on start."""
    from simulations.simple_assets import SimulatedAsset
    from src.asset_integration.models import AssetType as SimType
    from src.operator_app.adapters.mavlink_adapter import MAVLinkAdapter
    from src.operator_app.adapters.ros2_adapter import ROS2Adapter
    from src.operator_app.adapters.sim_asset_adapter import SimAssetAdapter

    adapters = []
    for spec in load_specs(path):
        if spec.adapter == "sim":
            kind = SimType.UAV if spec.type == "uav" else SimType.UGV
            sim = SimulatedAsset(spec.id, spec.name, kind, spec.home_lat, spec.home_lon, spec.home_alt)
            adapter = SimAssetAdapter(sim)
            adapter.asset.id = spec.id
        elif spec.adapter == "mavlink":
            adapter = MAVLinkAdapter.from_spec(spec)
        else:
            adapter = ROS2Adapter.from_spec(spec)
        adapters.append(adapter)
    return adapters
