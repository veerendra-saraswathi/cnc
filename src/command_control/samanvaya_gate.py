"""Only door from a SAMANVAYA Task into a CNC Command."""

from __future__ import annotations

from datetime import timezone

from src.asset_integration.models import Command, CommandType
from samanvaya.icd import Task


def command_from_task(task: Task) -> Command:
    issued = task.issued_at
    if issued.tzinfo is None:
        issued = issued.replace(tzinfo=timezone.utc)
    else:
        issued = issued.astimezone(timezone.utc)
    return Command(
        command_id=task.task_id[:8],
        asset_id=task.target_platform_id,
        type=CommandType(task.verb.value.lower()),
        parameters=task.parameters,
        issued_at=issued,
        issued_by=f"{task.role.value}:{task.issuer}",
    )