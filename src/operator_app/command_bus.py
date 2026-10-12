"""Dispatch cnc.command.<asset_id> and record cnc.ack.<asset_id> as an Event."""

from __future__ import annotations

import asyncio
import json
from typing import Callable

from loguru import logger
from pydantic import ValidationError

from src.data.bus import (
    PoisonMessage,
    PullConsumer,
    ack_timeout,
    current_mode,
    get_bus,
)
from src.data.subjects import (
    ACK_FILTER,
    ALERT_SUBJECT,
    COMMAND_FILTER,
    validation_reason,
)
from src.data_services.event_store import event_store
from src.data_services.repositories import get_repositories
from src.kernel.models import Alert, AlertLevel, Command, EventType
from src.operator_app.adapters.telemetry_publish import publish_command_ack

Lookup = Callable[[str], object]
_consumers: list[PullConsumer] = []
_lookup: Lookup | None = None
_pending: dict[str, Command] = {}
_gate = asyncio.Lock()


def _live() -> bool:
    return bool(_consumers) and all(consumer.running for consumer in _consumers)


async def start_command_bridge(lookup: Lookup) -> bool:
    """Subscribe to commands and acks. False when JetStream is not up."""
    global _lookup
    _lookup = lookup
    async with _gate:
        return await _start_command_bridge()


async def restart_command_bridge(lookup: Lookup | None = None) -> bool:
    """Replace pull consumers after the broker reconnects."""
    global _lookup
    if lookup is not None:
        _lookup = lookup
    async with _gate:
        await _stop_command_bridge()
        return await _start_command_bridge()


async def _start_command_bridge() -> bool:
    global _consumers
    if current_mode() != "nats":
        return False
    if _live():
        return True
    _consumers = []
    bus = get_bus()
    if not await bus.ensure_jetstream():
        return False
    started: list[PullConsumer] = []
    try:
        for stream, durable, subject, handler in (
            ("CNC_COMMAND", "cnc-commands", COMMAND_FILTER, _on_command),
            ("CNC_ACK", "cnc-acks", ACK_FILTER, _on_ack),
        ):
            consumer = PullConsumer(bus, stream, durable, subject, handler)
            if not await consumer.start():
                raise RuntimeError(f"{durable} did not start")
            started.append(consumer)
    except Exception:
        for consumer in started:
            await consumer.stop()
        raise
    _consumers = started
    return True


async def stop_command_bridge() -> None:
    async with _gate:
        await _stop_command_bridge()


async def _stop_command_bridge() -> None:
    global _consumers
    current = list(_consumers)
    _consumers = []
    for consumer in current:
        await consumer.stop()


def _watch(cmd: Command) -> None:
    _pending[cmd.id] = cmd


def _notify() -> None:
    from src.operator_app.live_feed import notify_live
    notify_live()


def command_audit_recorded(command_id: str) -> bool:
    """True only when the durable command Event row is readable."""
    try:
        return bool(get_repositories().command_event_recorded(command_id))
    except Exception as exc:
        logger.warning(f"[bus] command audit lookup failed: {exc}")
        return False


def remember_command(cmd: Command) -> None:
    try:
        get_repositories().update_command_result(cmd.id, cmd.status, cmd.result)
    except Exception as exc:
        logger.warning(f"[bus] command status was not stored: {exc}")


def _loaded_command(command_id: str) -> Command | None:
    try:
        from src.operator_app.app import platform
    except Exception:
        return None
    return platform.store.commands.get(command_id)


def _settle(command_id: str, ok: bool, reason: str) -> None:
    cmd = _pending.get(command_id) or _loaded_command(command_id)
    if cmd is None or cmd.status != "pending":
        return
    cmd.status = "sent" if ok else "failed"
    if ok:
        previous = dict(cmd.result or {})
        previous.pop("queued", None)
        previous["ack"] = True
        cmd.result = previous
    else:
        cmd.result = {"error": reason or "adapter rejected"}
    remember_command(cmd)
    _notify()


async def _send_local(platform, cmd: Command) -> None:
    if not command_audit_recorded(cmd.id):
        cmd.status = "failed"
        cmd.result = {"error": "command event was not recorded"}
        remember_command(cmd)
        _notify()
        return
    adapter = platform.adapters.get(cmd.asset_id)
    if adapter is None:
        cmd.status = "failed"
        cmd.result = {"error": "asset not found"}
        remember_command(cmd)
        _notify()
        return
    try:
        ok = await adapter.send_command(cmd)
    except Exception as exc:
        cmd.status = "failed"
        cmd.result = {"error": str(exc)}
        remember_command(cmd)
        _notify()
        return
    cmd.status = "sent" if ok else "failed"
    if ok:
        cmd.result = {"ack": True}
    elif not cmd.result:
        cmd.result = {"error": "adapter rejected"}
    remember_command(cmd)
    _notify()


async def dispatch_command(platform, cmd: Command) -> None:
    """In-process delivery when the bus is off. JetStream goes through the outbox.

    A missing durable command Event fails the command before any adapter runs.
    """
    if not command_audit_recorded(cmd.id):
        cmd.status = "failed"
        cmd.result = {"error": "command event was not recorded"}
        remember_command(cmd)
        _notify()
        return
    if current_mode() != "nats":
        await _send_local(platform, cmd)
        from src.data.outbox import mark_command_published
        mark_command_published(cmd.id)
        return

    from src.data.outbox import command_published, publish_pending

    bus = get_bus()
    bus.expect_ack(cmd.id)
    try:
        await publish_pending()
    except Exception:
        bus.forget_ack(cmd.id)
        raise
    if not command_published(cmd.id):
        bus.forget_ack(cmd.id)
        cmd.status = "pending"
        cmd.result = {"queued": True}
        _watch(cmd)
        remember_command(cmd)
        _notify()
        return
    cmd.status = "sent"
    remember_command(cmd)
    _notify()
    ack = await bus.wait_ack(cmd.id, ack_timeout())
    bus.forget_ack(cmd.id)
    if ack is None:
        alert = await record_ack_timeout(platform, cmd)
        cmd.status = "failed"
        cmd.result = {"error": "ack timeout", "alert_id": alert.id}
        remember_command(cmd)
        _notify()
        return
    if ack.get("ok"):
        cmd.status = "sent"
        cmd.result = {"ack": True}
    else:
        cmd.status = "failed"
        cmd.result = {"error": ack.get("reason") or "adapter rejected"}
    remember_command(cmd)
    _notify()


async def record_ack_timeout(platform, cmd: Command) -> Alert:
    kind = cmd.command_type.value if hasattr(cmd.command_type, "value") else str(cmd.command_type)
    alert = Alert(
        level=AlertLevel.WARNING,
        title="Command ack timeout",
        message=f"No ack for {kind} on {cmd.asset_id}",
        asset_id=cmd.asset_id,
        source="bus",
    )
    platform.store.alerts[alert.id] = alert
    try:
        get_repositories().alerts.insert(
            alert_id=alert.id,
            timestamp=alert.timestamp,
            asset_id=alert.asset_id,
            level=alert.level.value,
            message=alert.message,
            payload={"command_id": cmd.id, "reason": "ack timeout"},
        )
    except Exception as exc:
        logger.warning(f"[bus] ack timeout alert was not stored: {exc}")
    body = alert.model_dump(mode="json")
    body["command_id"] = cmd.id
    bus = get_bus()
    if not await bus.publish_jetstream(ALERT_SUBJECT, body):
        await bus.deliver_local(ALERT_SUBJECT, body)
    return alert


async def _on_command(subject: str, data: bytes) -> None:
    try:
        cmd = Command.model_validate_json(data)
    except ValidationError as exc:
        raise PoisonMessage(validation_reason(exc)) from exc
    adapter = _lookup(cmd.asset_id) if _lookup else None
    if adapter is None:
        # Nak so JetStream redelivers. A terminal ack here drops a command that
        # arrives while the demo fleet is still registering.
        raise RuntimeError("asset not found")
    if not command_audit_recorded(cmd.id):
        raise RuntimeError("command event was not recorded")
    try:
        await adapter.send_command(cmd)  # type: ignore[attr-defined]
    except Exception as exc:
        await publish_command_ack(cmd, False, str(exc))


async def _on_ack(subject: str, data: bytes) -> None:
    try:
        payload = json.loads(data.decode())
        command_id = str(payload["command_id"])
    except (UnicodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise PoisonMessage(str(exc)) from exc
    asset_id = payload.get("asset_id") or None
    kind = payload.get("command_type") or "command"
    ok = bool(payload.get("ok"))
    event_store.add(
        EventType.COMMAND,
        f"ack {kind} ({'ok' if ok else 'rejected'})",
        asset_id=asset_id,
        actor="adapter",
        data={
            "command_id": command_id,
            "ack": True,
            "ok": ok,
            "operator_id": "adapter",
            "reason": payload.get("reason") or "",
        },
    )
    get_bus().notify_ack(command_id, payload)
    _settle(command_id, ok, str(payload.get("reason") or ""))
