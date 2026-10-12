"""
Mission Coordinator – Canonical multi-asset orchestration layer
=============================================================
Uses only the platform's official models from asset_integration.
Consumes Telemetry, produces Commands.
Commands are issued only through the SAMANVAYA task gate.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set
import math
import uuid

from src.asset_integration.models import (
    AssetType,
    AssetStatus,
    Position,
    Telemetry,
    Command,
    CommandType,
)
from src.command_control.samanvaya_gate import command_from_task
from samanvaya.icd import AssetClass, CommandVerb, Role, Task as STask


# ------------------------------------------------------------------
# Internal task model (platform-level, not asset-level)
# ------------------------------------------------------------------
class TaskStatus(str, Enum):
    PLANNED = "planned"
    ASSIGNED = "assigned"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    REJECTED = "rejected"


@dataclass
class Task:
    task_id: str
    description: str
    target: Position
    preferred_types: Set[AssetType] = field(default_factory=set)
    assigned_asset: Optional[str] = None
    status: TaskStatus = TaskStatus.PLANNED
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class AssetRecord:
    """Live view of an asset inside the Coordinator."""
    asset_id: str
    asset_type: AssetType
    last_telemetry: Optional[Telemetry] = None
    current_task: Optional[str] = None
    status: AssetStatus = AssetStatus.OFFLINE


def distance_meters(p1: Position, p2: Position) -> float:
    """Haversine approximation (meters)."""
    R = 6371000  # Earth radius
    lat1, lon1 = math.radians(p1.lat), math.radians(p1.lon)
    lat2, lon2 = math.radians(p2.lat), math.radians(p2.lon)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))


class MissionCoordinator:
    def __init__(
        self,
        min_separation_m: float = 25.0,
        arrival_threshold_m: float = 12.0,
    ):
        self.assets: Dict[str, AssetRecord] = {}
        self.tasks: Dict[str, Task] = {}
        self.min_separation_m = min_separation_m
        self.arrival_threshold_m = arrival_threshold_m
        self.mission_id = str(uuid.uuid4())[:8]

    def register_asset(self, asset_id: str, asset_type: AssetType, initial_position: Position):
        self.assets[asset_id] = AssetRecord(
            asset_id=asset_id,
            asset_type=asset_type,
            last_telemetry=Telemetry(
                asset_id=asset_id,
                position=initial_position,
                status=AssetStatus.IDLE,
            ),
            status=AssetStatus.IDLE,
        )
        print(f"[Coordinator] Registered {asset_type.value.upper()} → {asset_id}")

    def update_telemetry(self, telemetry: Telemetry):
        record = self.assets.get(telemetry.asset_id)
        if not record:
            return

        record.last_telemetry = telemetry
        record.status = telemetry.status

        if record.current_task:
            task = self.tasks.get(record.current_task)
            if task and task.status == TaskStatus.EXECUTING:
                dist = distance_meters(telemetry.position, task.target)
                if dist <= self.arrival_threshold_m:
                    task.status = TaskStatus.COMPLETED
                    task.completed_at = datetime.now(timezone.utc)
                    record.current_task = None
                    record.status = AssetStatus.IDLE
                    print(f"[Coordinator] Task {task.task_id} COMPLETED by {record.asset_id}")

    def create_task(
        self,
        description: str,
        target: Position,
        preferred_types: Optional[List[AssetType]] = None,
    ) -> str:
        task_id = f"T{len(self.tasks)+1:03d}"
        self.tasks[task_id] = Task(
            task_id=task_id,
            description=description,
            target=target,
            preferred_types=set(preferred_types or []),
        )
        print(f"[Coordinator] Created {task_id}: {description}")
        return task_id

    def assign_pending_tasks(self) -> List[str]:
        """Returns list of newly assigned task_ids."""
        newly_assigned = []

        for task in self.tasks.values():
            if task.status != TaskStatus.PLANNED:
                continue

            candidates = []
            for record in self.assets.values():
                if record.status not in (AssetStatus.IDLE, AssetStatus.ACTIVE):
                    continue
                if task.preferred_types and record.asset_type not in task.preferred_types:
                    continue
                if record.last_telemetry is None:
                    continue
                candidates.append(record)

            best = None
            best_dist = float("inf")

            for record in candidates:
                dist = distance_meters(record.last_telemetry.position, task.target)
                if dist < best_dist and self._is_safe(record, task.target):
                    best = record
                    best_dist = dist

            if best:
                task.assigned_asset = best.asset_id
                task.status = TaskStatus.ASSIGNED
                best.current_task = task.task_id
                best.status = AssetStatus.MISSION
                newly_assigned.append(task.task_id)
                print(f"[Coordinator] ASSIGNED {task.task_id} → {best.asset_id}")
            else:
                task.status = TaskStatus.REJECTED
                print(f"[Coordinator] REJECTED {task.task_id} (no safe asset available)")

        return newly_assigned

    def _is_safe(self, record: AssetRecord, target: Position) -> bool:
        for other in self.assets.values():
            if other.asset_id == record.asset_id:
                continue
            if other.last_telemetry is None:
                continue
            if distance_meters(other.last_telemetry.position, target) < self.min_separation_m:
                return False
        return True

    def _asset_class(self, asset_type: AssetType) -> AssetClass:
        if asset_type == AssetType.UAV:
            return AssetClass.UAV
        if asset_type == AssetType.UGV:
            return AssetClass.UGV
        raise ValueError(f"{asset_type.value} is not a Phase 1 class")

    def start_assigned_tasks(self) -> List[Command]:
        commands: List[Command] = []

        for task in self.tasks.values():
            if task.status != TaskStatus.ASSIGNED or not task.assigned_asset:
                continue

            task.status = TaskStatus.EXECUTING
            task.started_at = datetime.now(timezone.utc)
            record = self.assets[task.assigned_asset]
            stask = STask(
                issuer="coordinator",
                role=Role.SUPERVISOR,
                target_platform_id=task.assigned_asset,
                asset_class=self._asset_class(record.asset_type),
                verb=CommandVerb.GOTO,
                parameters={
                    "lat": task.target.lat,
                    "lon": task.target.lon,
                    "alt": task.target.alt,
                },
            )
            cmd = command_from_task(stask)
            commands.append(cmd)
            print(f"[Coordinator] EXECUTING {task.task_id} on {task.assigned_asset} by {cmd.issued_by}")

        return commands

    def get_summary(self) -> dict:
        return {
            "mission_id": self.mission_id,
            "assets": {
                aid: {
                    "type": r.asset_type.value,
                    "status": r.status.value,
                    "task": r.current_task,
                    "position": {
                        "lat": r.last_telemetry.position.lat if r.last_telemetry else None,
                        "lon": r.last_telemetry.position.lon if r.last_telemetry else None,
                        "alt": r.last_telemetry.position.alt if r.last_telemetry else None,
                    } if r.last_telemetry else None,
                }
                for aid, r in self.assets.items()
            },
            "tasks": {
                tid: {
                    "description": t.description,
                    "status": t.status.value,
                    "assigned": t.assigned_asset,
                    "target": {"lat": t.target.lat, "lon": t.target.lon, "alt": t.target.alt},
                }
                for tid, t in self.tasks.items()
            },
        }


if __name__ == "__main__":
    from pprint import pprint

    coord = MissionCoordinator(min_separation_m=30)

    coord.register_asset("UAV-01", AssetType.UAV, Position(lat=17.3850, lon=78.4867, alt=80))
    coord.register_asset("UAV-02", AssetType.UAV, Position(lat=17.3900, lon=78.4920, alt=90))
    coord.register_asset("UGV-01", AssetType.UGV, Position(lat=17.3800, lon=78.4800, alt=0))
    coord.register_asset("UGV-02", AssetType.UGV, Position(lat=17.3750, lon=78.4950, alt=0))

    coord.create_task(
        "Aerial overwatch North",
        Position(lat=17.4000, lon=78.5000, alt=100),
        [AssetType.UAV]
    )
    coord.create_task(
        "Aerial overwatch East",
        Position(lat=17.3950, lon=78.5100, alt=95),
        [AssetType.UAV]
    )
    coord.create_task(
        "Ground secure point",
        Position(lat=17.3880, lon=78.4880, alt=0),
        [AssetType.UGV]
    )
    coord.create_task(
        "Ground recon South",
        Position(lat=17.3700, lon=78.4900, alt=0),
        [AssetType.UGV]
    )

    coord.assign_pending_tasks()
    commands = coord.start_assigned_tasks()

    print("\nGenerated Commands:")
    for c in commands:
        print(f"  → {c.asset_id} | {c.type.value} | {c.issued_by} | {c.parameters}")

    print("\nSummary:")
    pprint(coord.get_summary())
    