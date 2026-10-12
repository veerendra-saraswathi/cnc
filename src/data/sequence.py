"""Per-asset monotonic sequence numbers for commands and telemetry.

The live snapshot keeps the highest telemetry sequence it has applied.
An older sample is still stored; it does not move the hot position backward.
"""

from __future__ import annotations

import threading

_lock = threading.Lock()
_command: dict[str, int] = {}
_telemetry: dict[str, int] = {}
_live: dict[str, int] = {}


def next_command_seq(asset_id: str) -> int:
    with _lock:
        value = _command.get(asset_id, 0) + 1
        _command[asset_id] = value
        return value


def next_telemetry_seq(asset_id: str) -> int:
    with _lock:
        value = _telemetry.get(asset_id, 0) + 1
        _telemetry[asset_id] = value
        return value


def live_telemetry_stale(asset_id: str, seq: int) -> bool:
    """True when this sample is older than the snapshot already applied."""
    with _lock:
        last = _live.get(asset_id)
        return last is not None and seq <= last


def remember_live_telemetry(asset_id: str, seq: int) -> None:
    with _lock:
        last = _live.get(asset_id)
        if last is None or seq > last:
            _live[asset_id] = seq


def reset_sequences() -> None:
    with _lock:
        _command.clear()
        _telemetry.clear()
        _live.clear()
