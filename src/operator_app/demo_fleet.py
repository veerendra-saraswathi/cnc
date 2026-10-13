"""Demo fleet from fleet.yaml, on the telemetry bus.

The default file is two sim UAVs and two sim UGVs. CNC_FLEET can point at a
mix of sim, MAVLink, and ROS 2. Callers that construct SimAssetAdapter(sim)
themselves still get a UUID.
"""

from __future__ import annotations

from src.data.subjects import telemetry_subject
from src.operator_app.fleet_config import load_fleet_adapters


def demo_adapters() -> list:
    return load_fleet_adapters()


def demo_subjects(adapters: list | None = None) -> list[str]:
    chosen = demo_adapters() if adapters is None else adapters
    return [telemetry_subject(adapter.id) for adapter in chosen]


def demo_homes(adapters: list | None = None) -> dict[str, tuple[float, float, float]]:
    chosen = demo_adapters() if adapters is None else adapters
    return {
        adapter.id: (adapter.asset.home_lat, adapter.asset.home_lon, adapter.asset.alt)
        for adapter in chosen
    }


