"""
MQTT Simulation Runner
- Publishes live telemetry
- Listens for commands on MQTT
"""

import time
import sys
import json
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from simulations.simple_assets import create_demo_fleet
from src.communication.mqtt_client import MQTTClient
from rich.console import Console

console = Console()


def main():
    console.print("[bold green]Starting MQTT Simulation (with command listening)...[/bold green]")

    fleet = create_demo_fleet()

    # Initial movement
    fleet["uav-01"].send_command("goto", {"lat": 17.3950, "lon": 78.5000, "alt": 100})
    fleet["uav-02"].send_command("goto", {"lat": 17.4000, "lon": 78.4800, "alt": 120})
    fleet["ugv-01"].send_command("goto", {"lat": 17.3900, "lon": 78.4950})
    fleet["ugv-02"].send_command("goto", {"lat": 17.3750, "lon": 78.4750})

    mqtt = MQTTClient(client_id="cnc-simulator")

    def on_command(topic, payload):
        """Called when a command arrives on MQTT."""
        try:
            # topic format: assets/{asset_id}/command
            parts = topic.split("/")
            if len(parts) >= 3:
                asset_id = parts[1]
                if asset_id in fleet:
                    cmd_type = payload.get("type") or payload.get("command_type")
                    params = payload.get("parameters", {})
                    console.print(f"[cyan]Received command for {asset_id}: {cmd_type} {params}[/cyan]")
                    fleet[asset_id].send_command(cmd_type, params)
        except Exception as e:
            console.print(f"[red]Error handling command: {e}[/red]")

    try:
        mqtt.connect()
        # Subscribe to all asset command topics
        mqtt.subscribe("assets/+/command", on_command)
        console.print("[green]Connected to MQTT and listening for commands[/green]")
    except Exception as e:
        console.print(f"[yellow]MQTT not available: {e}[/yellow]")
        console.print("[yellow]Running offline (no MQTT)[/yellow]")
        mqtt = None

    console.print("Publishing telemetry... Press Ctrl+C to stop.\n")

    try:
        while True:
            for asset_id, asset in fleet.items():
                asset.update(dt=1.0)
                telemetry = asset.get_telemetry()
                payload = telemetry.model_dump(mode="json")

                if mqtt:
                    mqtt.publish_telemetry(asset_id, payload)

                console.print(
                    f"{asset.info.name:8} | {telemetry.status.value:10} | "
                    f"Bat {telemetry.battery_percent:5.1f}% | "
                    f"Spd {telemetry.speed:4.1f} | "
                    f"{telemetry.position.lat:.5f}, {telemetry.position.lon:.5f}"
                )

            time.sleep(1.0)

    except KeyboardInterrupt:
        console.print("\n[bold red]Simulation stopped.[/bold red]")
        if mqtt:
            mqtt.disconnect()


if __name__ == "__main__":
    main()
    