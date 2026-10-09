"""Section 10 proof: 2 UAV + 2 UGV, one tasking pass, audit tape.

One laptop. No dashboard and no MQTT required.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.asset_integration.models import AssetType, Position
from src.command_control.coordinator import MissionCoordinator
from src.security import AuditLog, Role, Session


def main() -> None:
    log = AuditLog()
    session = Session(issuer="op-1", role=Role.OPERATOR)
    coord = MissionCoordinator(min_separation_m=30)

    coord.register_asset("UAV-01", AssetType.UAV, Position(lat=17.3850, lon=78.4867, alt=80))
    coord.register_asset("UAV-02", AssetType.UAV, Position(lat=17.3900, lon=78.4920, alt=90))
    coord.register_asset("UGV-01", AssetType.UGV, Position(lat=17.3800, lon=78.4800, alt=0))
    coord.register_asset("UGV-02", AssetType.UGV, Position(lat=17.3750, lon=78.4950, alt=0))

    coord.create_task("Aerial overwatch North", Position(lat=17.4000, lon=78.5000, alt=100), [AssetType.UAV])
    coord.create_task("Aerial overwatch East", Position(lat=17.3950, lon=78.5100, alt=95), [AssetType.UAV])
    coord.create_task("Ground secure point", Position(lat=17.3880, lon=78.4880, alt=0), [AssetType.UGV])
    coord.create_task("Ground recon South", Position(lat=17.3700, lon=78.4900, alt=0), [AssetType.UGV])

    assigned = coord.assign_pending_tasks()
    for task_id in assigned:
        task = coord.tasks[task_id]
        log.authorise(session, task.assigned_asset, "goto")

    commands = coord.start_assigned_tasks()
    out = Path("docs/acceptance_run.json")
    out.write_text(json.dumps({
        "assets": sorted(coord.assets),
        "assigned": assigned,
        "commands": [
            {"asset_id": c.asset_id, "verb": c.type.value, "issued_by": c.issued_by}
            for c in commands
        ],
        "audit": [e.model_dump(mode="json") for e in log.entries],
    }, indent=2))
    print(f"assets={len(coord.assets)} commands={len(commands)} tape={out}")
    for c in commands:
        print(f"  {c.asset_id} {c.type.value} {c.issued_by}")


if __name__ == "__main__":
    main()
