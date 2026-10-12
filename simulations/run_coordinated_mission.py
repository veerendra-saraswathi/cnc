"""
Coordinated Multi-Asset Mission Runner + MQTT Publishing
=======================================================
Publishes:
  - assets/{id}/telemetry
  - mission/status
"""

from __future__ import annotations
import time
import json
from pprint import pprint
from datetime import datetime, timezone

from src.command_control.coordinator import MissionCoordinator
from src.asset_integration.models import Position, AssetType, Telemetry
from src.communication.mqtt_client import MQTTClient
from simulations.multi_asset_world import MultiAssetWorld


def publish_telemetry(mqtt: MQTTClient, telemetry_list: list[Telemetry]):
    for t in telemetry_list:
        payload = {
            "asset_id": t.asset_id,
            "timestamp": t.timestamp.isoformat() if hasattr(t.timestamp, "isoformat") else str(t.timestamp),
            "lat": t.position.lat,
            "lon": t.position.lon,
            "alt": t.position.alt,
            "speed": t.speed,
            "status": t.status.value if hasattr(t.status, "value") else str(t.status),
            "mode": t.mode,
        }
        mqtt.publish_telemetry(t.asset_id, payload)


def publish_mission_status(mqtt: MQTTClient, summary: dict):
    payload = {
        "mission_id": summary["mission_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "assets": summary["assets"],
        "tasks": summary["tasks"],
    }
    mqtt.publish("mission/status", payload)


def main():
    print("=" * 60)
    print("  Unified Mission Autonomy – Coordinated Mission + MQTT")
    print("=" * 60)

    # MQTT
    mqtt = MQTTClient(client_id="cnc-coordinator")
    try:
        mqtt.connect()
        print("[MQTT] Connected")
    except Exception as e:
        print(f"[MQTT] Could not connect ({e}). Continuing without MQTT.")
        mqtt = None

    # Digital twin + Coordinator
    world = MultiAssetWorld()
    coord = MissionCoordinator(min_separation_m=12.0, arrival_threshold_m=35.0)
    world.register_with_coordinator(coord)

    print("\n[Mission] Creating tasks...")
    coord.create_task("Aerial overwatch North", Position(lat=17.3875, lon=78.4930, alt=95), [AssetType.UAV])
    coord.create_task("Aerial overwatch East",  Position(lat=17.3858, lon=78.4925, alt=90), [AssetType.UAV])
    coord.create_task("Ground secure point",    Position(lat=17.3812, lon=78.4815, alt=0),  [AssetType.UGV])
    coord.create_task("Ground recon South",     Position(lat=17.3762, lon=78.4940, alt=0),  [AssetType.UGV])

    print("\n[Mission] Assigning + starting...")
    coord.assign_pending_tasks()
    commands = coord.start_assigned_tasks()
    world.apply_commands(commands)

    print("\n--- Live Simulation + MQTT Publishing ---")
    for step in range(1, 22):
        world.step(dt=2.5)

        telemetry = world.get_telemetry()
        for t in telemetry:
            coord.update_telemetry(t)

        summary = coord.get_summary()

        # Publish to MQTT
        if mqtt:
            publish_telemetry(mqtt, telemetry)
            publish_mission_status(mqtt, summary)

        statuses = [f"{tid}:{t['status'][:4]}" for tid, t in summary["tasks"].items()]
        print(f"Step {step:02d} | {' | '.join(statuses)}")

        if all(t["status"] == "completed" for t in summary["tasks"].values()):
            print("\n All 4 tasks completed successfully.")
            if mqtt:
                publish_mission_status(mqtt, summary)  # final status
            break

        time.sleep(0.15)

    print("\n" + "=" * 60)
    print("  FINAL MISSION SUMMARY")
    print("=" * 60)
    pprint(coord.get_summary(), sort_dicts=False)

    if mqtt:
        print("\n[MQTT] Final telemetry + mission status published.")
        time.sleep(1)
        mqtt.disconnect()


if __name__ == "__main__":
    main()
    