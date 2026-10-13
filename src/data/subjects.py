"""NATS subjects and one JetStream stream per family.

Retention is file storage, 24 hours, discard old. Command, ack, event,
alert, and DLQ streams stay at 1 GiB. Telemetry is sized for four assets
at 10 Hz for that same 24 hours, so discard-old does not drop a full-rate
day before max age. There is no max_msgs cap. Retention policy is limits
so ingest and fusion can both read telemetry. A work-queue stream would
delete a message on the first ack.
"""

from __future__ import annotations

from dataclasses import dataclass

MAX_AGE_SECONDS = 24 * 60 * 60
MAX_BYTES = 1024 ** 3
# Four vehicles, the MAVLink stream request, one day, 2 KiB per sample.
# A kernel JSON sample is under 1 KiB. The extra KiB is headroom.
TELEMETRY_ASSETS = 4
TELEMETRY_HZ = 10
TELEMETRY_MSG_BYTES = 2048
TELEMETRY_MAX_BYTES = TELEMETRY_ASSETS * TELEMETRY_HZ * MAX_AGE_SECONDS * TELEMETRY_MSG_BYTES

TELEMETRY_PREFIX = "cnc.telemetry."
COMMAND_PREFIX = "cnc.command."
ACK_PREFIX = "cnc.ack."
EVENT_SUBJECT = "cnc.event"
ALERT_SUBJECT = "cnc.alert"
DLQ_SUBJECT = "cnc.dlq"

TELEMETRY_FILTER = "cnc.telemetry.>"
COMMAND_FILTER = "cnc.command.>"
ACK_FILTER = "cnc.ack.>"


@dataclass(frozen=True)
class StreamSpec:
    name: str
    subjects: tuple[str, ...]
    max_age_seconds: float = MAX_AGE_SECONDS
    max_bytes: int = MAX_BYTES
    storage: str = "file"
    discard: str = "old"
    duplicate_window: float = 0


# JetStream drops a second publish with the same Nats-Msg-Id inside this window.
COMMAND_DUPLICATE_WINDOW = 120.0


STREAMS: tuple[StreamSpec, ...] = (
    StreamSpec("CNC_TELEMETRY", (TELEMETRY_FILTER,), max_bytes=TELEMETRY_MAX_BYTES),
    StreamSpec("CNC_COMMAND", (COMMAND_FILTER,), duplicate_window=COMMAND_DUPLICATE_WINDOW),
    StreamSpec("CNC_ACK", (ACK_FILTER,)),
    StreamSpec("CNC_EVENT", (EVENT_SUBJECT,)),
    StreamSpec("CNC_ALERT", (ALERT_SUBJECT,)),
    StreamSpec("CNC_DLQ", (DLQ_SUBJECT,)),
)

_BAD_TOKEN = set(".*> \t\r\n")


def token(asset_id: str) -> str:
    """One NATS token. Dots and wildcards would escape the asset subject."""
    if not asset_id or any(char in _BAD_TOKEN or char.isspace() for char in asset_id):
        raise ValueError(f"asset_id is not a subject token: {asset_id!r}")
    return asset_id


def telemetry_subject(asset_id: str) -> str:
    return f"{TELEMETRY_PREFIX}{token(asset_id)}"


def command_subject(asset_id: str) -> str:
    return f"{COMMAND_PREFIX}{token(asset_id)}"


def ack_subject(asset_id: str) -> str:
    return f"{ACK_PREFIX}{token(asset_id)}"


def subject_matches(pattern: str, subject: str) -> bool:
    if pattern == subject:
        return True
    if pattern.endswith(".>"):
        return subject.startswith(pattern[:-1])
    if pattern.endswith(".*"):
        prefix = pattern[:-1]
        if not subject.startswith(prefix):
            return False
        rest = subject[len(prefix):]
        return bool(rest) and "." not in rest
    return False


def validation_reason(exc: Exception) -> str:
    errors = getattr(exc, "errors", None)
    if not callable(errors):
        return str(exc)
    parts: list[str] = []
    for err in errors():
        loc = ".".join(str(item) for item in err.get("loc", ()))
        msg = err.get("msg", "invalid")
        parts.append(f"{loc}: {msg}" if loc else str(msg))
    return "; ".join(parts) or str(exc)
