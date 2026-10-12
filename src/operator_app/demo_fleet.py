"""Demo fleet: two sim UAVs and two sim UGVs on the telemetry bus.

SimAssetAdapter keeps a fresh id unless this factory assigns the sim id.
Callers that construct SimAssetAdapter(sim) themselves still get a UUID.
"""

from __future__ import annotations

from simulations.simple_assets import create_demo_fleet
from src.data.subjects import telemetry_subject
from src.operator_app.adapters.sim_asset_adapter import SimAssetAdapter


def demo_adapters() -> list[SimAssetAdapter]:
    adapters: list[SimAssetAdapter] = []
    for sim in create_demo_fleet().values():
        adapter = SimAssetAdapter(sim)
        adapter.asset.id = sim.info.asset_id
        adapters.append(adapter)
    return adapters


def demo_subjects(adapters: list[SimAssetAdapter] | None = None) -> list[str]:
    chosen = demo_adapters() if adapters is None else adapters
    return [telemetry_subject(adapter.id) for adapter in chosen]


def demo_homes(adapters: list[SimAssetAdapter] | None = None) -> dict[str, tuple[float, float, float]]:
    chosen = demo_adapters() if adapters is None else adapters
    return {
        adapter.id: (adapter.asset.home_lat, adapter.asset.home_lon, adapter.asset.alt)
        for adapter in chosen
    }


