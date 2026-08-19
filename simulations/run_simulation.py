"""
Live simulation runner.
Runs the 2 UAV + 2 UGV fleet and prints live telemetry.
Later we will publish this over MQTT.
"""

import time
import sys
from pathlib import Path

# Allow importing from src/
sys.path.append(str(Path(__file__).parent.parent))

from simulations.simple_assets import create_demo_fleet
from rich.console import Console
from rich.table import Table
from rich.live import Live

console = Console()


def make_table(fleet):
    table = Table(title="CNC – Live Asset Status", show_header=True, header_style="bold cyan")
    table.add_column("Asset ID", style="bold")
    table.add_column("Name")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Lat", justify="right")
    table.add_column("Lon", justify="right")
    table.add_column("Alt", justify="right")
    table.add_column("Speed", justify="right")
    table.add_column("Battery", justify="right")

    for asset in fleet.values():
        t = asset.get_telemetry()
        table.add_row(
            t.asset_id,
            asset.info.name,
            asset.info.asset_type.value.upper(),
            t.status.value,
            f"{t.position.lat:.5f}",
            f"{t.position.lon:.5f}",
            f"{t.position.alt:.1f}",
            f"{t.speed:.1f}",
            f"{t.battery_percent:.1f}%",
        )
    return table


def main():
    fleet = create_demo_fleet()
    console.print("[bold green]Starting CNC Simulation...[/bold green]")
    console.print("Press Ctrl+C to stop\n")

    # Give some initial movement commands so the assets actually move
    fleet["uav-01"].send_command("goto", {"lat": 17.3950, "lon": 78.5000, "alt": 100})
    fleet["uav-02"].send_command("goto", {"lat": 17.4000, "lon": 78.4800, "alt": 120})
    fleet["ugv-01"].send_command("goto", {"lat": 17.3900, "lon": 78.4950})
    fleet["ugv-02"].send_command("goto", {"lat": 17.3750, "lon": 78.4750})

    try:
        with Live(make_table(fleet), refresh_per_second=2, console=console) as live:
            while True:
                for asset in fleet.values():
                    asset.update(dt=0.5)
                live.update(make_table(fleet))
                time.sleep(0.5)
    except KeyboardInterrupt:
        console.print("\n[bold red]Simulation stopped.[/bold red]")


if __name__ == "__main__":
    main()
    