"""Validate adapter telemetry with the kernel model, then publish or dead-letter.

JetStream acks are awaited on a bounded set of tasks. The picture loop does
not wait for each one. A failed publish still delivers locally and raises
one alert, not one alert per sample.
"""

from __future__ import annotations

import asyncio
import time

from pydantic import ValidationError

from src.data.bus import get_bus
from src.data.sequence import next_telemetry_seq
from src.data.subjects import ack_subject, telemetry_subject, validation_reason
from src.kernel.models import Command, Telemetry

# In-flight PubAcks. 32 covers 4 assets at 10 Hz when an ack takes under 100 ms.
INFLIGHT_LIMIT = 32
PUBLISH_TIMEOUT_S = 2.0
_ALERT_GAP_S = 5.0

_inflight = asyncio.Semaphore(INFLIGHT_LIMIT)
_pending: set[asyncio.Task] = set()
_fail_count = 0
_last_alert = 0.0
_alert_lock = asyncio.Lock()


def _reset_publish_state() -> None:
    """Tests clear the failure counter and the in-flight gate."""
    global _fail_count, _last_alert, _inflight
    _fail_count = 0
    _last_alert = 0.0
    _inflight = asyncio.Semaphore(INFLIGHT_LIMIT)


def _stamp_nats(model: Telemetry) -> None:
    timing = (model.extras or {}).get("timing") if isinstance(model.extras, dict) else None
    poll_ms = timing.get("poll_ms") if isinstance(timing, dict) else None
    if poll_ms is None:
        return
    from src.operator_app.telem_timing import note_nats, now_ms

    note_nats(model.asset_id, poll_ms, now_ms())


async def _raise_publish_alert(asset_id: str, message: str) -> None:
    from src.kernel.models import Alert, AlertLevel
    from src.operator_app.app import platform

    await platform.store.add_alert(Alert(
        id="telemetry-publish",
        level=AlertLevel.WARNING,
        title="Telemetry publish failed",
        message=message,
        asset_id=asset_id,
        source="bus",
    ))


async def _note_publish_failure(asset_id: str, reason: str) -> None:
    """One dashboard alert. Further failures within 5 s only increment the count."""
    global _fail_count, _last_alert
    async with _alert_lock:
        _fail_count += 1
        now = time.monotonic()
        if _last_alert and now - _last_alert < _ALERT_GAP_S:
            return
        _last_alert = now
        count = _fail_count
    text = f"JetStream did not accept telemetry ({count} failures), last {asset_id}: {reason[:160]}"
    try:
        await _raise_publish_alert(asset_id, text)
    except Exception:
        pass


async def _publish_one(bus, subject: str, payload: dict, model: Telemetry) -> None:
    try:
        async with _inflight:
            ok = await bus.publish_jetstream(subject, payload, timeout=PUBLISH_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        try:
            await bus.deliver_local(subject, payload)
        except Exception:
            pass
        await _note_publish_failure(model.asset_id, str(exc) or exc.__class__.__name__)
        _stamp_nats(model)
        return
    if not ok:
        try:
            await bus.deliver_local(subject, payload)
        except Exception:
            pass
        await _note_publish_failure(model.asset_id, "publish rejected")
    _stamp_nats(model)


def _schedule(bus, subject: str, payload: dict, model: Telemetry) -> None:
    task = asyncio.create_task(_publish_one(bus, subject, payload, model), name=f"telem-{model.asset_id}")
    _pending.add(task)
    task.add_done_callback(_pending.discard)


async def drain_telemetry_publishes() -> None:
    """Cancel in-flight telemetry publishes. Called before the bus closes."""
    tasks = list(_pending)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.wait(tasks, timeout=1.0)


async def accept_vendor_telemetry(raw) -> Telemetry | None:
    """Return the kernel sample. Invalid payloads are published on cnc.dlq.

    In-process mode still awaits the local handoff, so tests see the sample
    before this returns. NATS mode schedules a bounded ack and returns.
    """
    bus = get_bus()
    try:
        if isinstance(raw, Telemetry):
            model = Telemetry.model_validate(raw.model_dump())
        else:
            model = Telemetry.model_validate(raw)
        subject = telemetry_subject(model.asset_id)
        if model.seq <= 0:
            model.seq = next_telemetry_seq(model.asset_id)
    except (ValidationError, ValueError) as exc:
        reason = validation_reason(exc)
        await bus.publish_dlq(reason=reason, payload=raw if not isinstance(raw, Telemetry) else raw.model_dump(mode="json"), subject="cnc.telemetry")
        return None
    payload = model.model_dump(mode="json")
    if bus.mode != "nats":
        if not await bus.publish_jetstream(subject, payload, timeout=PUBLISH_TIMEOUT_S):
            await bus.deliver_local(subject, payload)
        _stamp_nats(model)
        return model
    _schedule(bus, subject, payload, model)
    return model


async def publish_command_ack(command: Command, ok: bool, reason: str = "") -> None:
    """Adapters reply here. In-process mode with no subscriber does not dial."""
    bus = get_bus()
    kind = command.command_type.value if hasattr(command.command_type, "value") else str(command.command_type)
    try:
        subject = ack_subject(command.asset_id)
    except ValueError:
        return
    payload = {
        "command_id": command.id,
        "asset_id": command.asset_id,
        "ok": ok,
        "reason": reason,
        "command_type": kind,
        "actor": "adapter",
    }
    if bus.mode != "nats" and not bus.has_local(subject):
        return
    if not await bus.publish_jetstream(subject, payload):
        await bus.deliver_local(subject, payload)
