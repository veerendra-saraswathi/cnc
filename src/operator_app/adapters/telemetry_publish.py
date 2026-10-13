"""Validate adapter telemetry with the kernel model, then publish or dead-letter."""

from __future__ import annotations

from pydantic import ValidationError

from src.data.bus import get_bus
from src.data.sequence import next_telemetry_seq
from src.data.subjects import ack_subject, telemetry_subject, validation_reason
from src.kernel.models import Command, Telemetry


async def accept_vendor_telemetry(raw) -> Telemetry | None:
    """Return the kernel sample. Invalid payloads are published on cnc.dlq."""
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
    # A stuck broker must not hold the telemetry worker. Two seconds still bounds a local JetStream ack.
    if not await bus.publish_jetstream(subject, payload, timeout=2.0):
        await bus.deliver_local(subject, payload)
    timing = (model.extras or {}).get("timing") if isinstance(model.extras, dict) else None
    poll_ms = timing.get("poll_ms") if isinstance(timing, dict) else None
    if poll_ms is not None:
        from src.operator_app.telem_timing import note_nats, now_ms

        note_nats(model.asset_id, poll_ms, now_ms())
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
