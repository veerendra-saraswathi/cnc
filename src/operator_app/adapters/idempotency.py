"""One execution per asset and command id.

A duplicate delivery is acknowledged again with the original result.
The vehicle command is not applied a second time.
"""

from __future__ import annotations

from typing import Awaitable, Callable

from src.kernel.models import Command
from src.operator_app.adapters.telemetry_publish import publish_command_ack

Apply = Callable[[Command], Awaitable[bool]]
_done: dict[str, dict[str, tuple[bool, str]]] = {}


def recall(asset_id: str, command_id: str) -> tuple[bool, str] | None:
    return _done.get(asset_id, {}).get(command_id)


def remember(asset_id: str, command_id: str, ok: bool, reason: str) -> None:
    _done.setdefault(asset_id, {})[command_id] = (ok, reason)


def reset_processed() -> None:
    _done.clear()


async def deliver_once(apply: Apply, command: Command) -> bool:
    prior = recall(command.asset_id, command.id)
    if prior is not None:
        ok, reason = prior
        await publish_command_ack(command, ok, reason or "duplicate")
        return ok
    try:
        outcome = await apply(command)
        if isinstance(outcome, tuple):
            ok, reason = bool(outcome[0]), str(outcome[1])
        else:
            ok = bool(outcome)
            reason = "" if ok else "rejected"
    except Exception as exc:
        ok = False
        reason = str(exc)
    remember(command.asset_id, command.id, ok, reason)
    await publish_command_ack(command, ok, reason)
    return ok
