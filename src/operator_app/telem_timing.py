"""Stage stamps for one telemetry sample.

The picture path reads the latest row when it builds a frame. JetStream and
ingest write their stamps later, on the same sample key. UAV position messages
have no unix time, so source_ms stays empty and message_gap_ms is the stream
interval.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from collections import deque

_LOCK = threading.Lock()
_LIVE: dict[str, dict] = {}
_ROWS: deque[dict] = deque()
_PIPE: dict[tuple[str, int], dict] = {}
_PIPE_ORDER: deque[tuple[str, int]] = deque()
_DROPPED = 0
_CAP = 80_000

_ROW_METRICS = (
    "source_to_adapter",
    "adapter_to_poll",
    "poll_to_frame",
    "message_gap",
)
_PIPE_METRICS = (
    "poll_to_nats",
    "nats_to_ingest",
)


def now_ms() -> float:
    return time.time() * 1000.0


def reset() -> None:
    """Clear stamps. Tests call this so one module does not leak into the next."""
    global _DROPPED
    with _LOCK:
        _LIVE.clear()
        _ROWS.clear()
        _PIPE.clear()
        _PIPE_ORDER.clear()
        _DROPPED = 0


def _kind(value) -> str:
    text = getattr(value, "value", value)
    text = str(text or "other").lower()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    if text in {"uav", "ugv"}:
        return text
    return "other"


def _num(value) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _span(start, end) -> float | None:
    left = _num(start)
    right = _num(end)
    if left is None or right is None:
        return None
    return right - left


def _key(asset_id: str, poll_ms: float) -> tuple[str, int]:
    return str(asset_id), int(round(float(poll_ms)))


def _trim_rows() -> None:
    while len(_ROWS) > _CAP:
        _ROWS.popleft()


def _trim_pipe() -> None:
    while len(_PIPE_ORDER) > _CAP:
        old = _PIPE_ORDER.popleft()
        _PIPE.pop(old, None)


def note_poll(
    asset_id: str,
    asset_type,
    source_ms,
    adapter_ms,
    poll_ms,
    message_gap_ms=None,
) -> None:
    row = {
        "asset_id": str(asset_id),
        "asset_type": _kind(asset_type),
        "source_ms": _num(source_ms),
        "adapter_ms": _num(adapter_ms),
        "poll_ms": float(poll_ms),
        "message_gap_ms": _num(message_gap_ms),
        "frame_ms": None,
    }
    with _LOCK:
        _LIVE[row["asset_id"]] = row
        _ROWS.append(row)
        _trim_rows()


def stages_for(asset_id: str) -> dict | None:
    with _LOCK:
        row = _LIVE.get(str(asset_id))
        if row is None:
            return None
        return dict(row)


def stamp_frame(asset_id: str, frame_ms: float) -> None:
    """First frame that sees this poll wins. A later push must not stretch the sample."""
    with _LOCK:
        row = _LIVE.get(str(asset_id))
        if row is None or row.get("frame_ms") is not None:
            return
        row["frame_ms"] = float(frame_ms)


def _pipe_row(asset_id: str, poll_ms: float) -> dict:
    key = _key(asset_id, poll_ms)
    row = _PIPE.get(key)
    if row is None:
        live = _LIVE.get(str(asset_id))
        row = {
            "asset_id": str(asset_id),
            "asset_type": (live or {}).get("asset_type") or "other",
            "poll_ms": float(poll_ms),
        }
        _PIPE[key] = row
        _PIPE_ORDER.append(key)
        _trim_pipe()
    return row


def note_nats(asset_id: str, poll_ms, nats_ms: float) -> None:
    poll = _num(poll_ms)
    if poll is None:
        return
    with _LOCK:
        _pipe_row(asset_id, poll)["nats_ms"] = float(nats_ms)


def note_ingest(asset_id: str, poll_ms, ingest_ms: float) -> None:
    poll = _num(poll_ms)
    if poll is None:
        return
    with _LOCK:
        _pipe_row(asset_id, poll)["ingest_ms"] = float(ingest_ms)


def offer(queue, item) -> bool:
    """Enqueue item. A full queue drops the oldest item so the picture path never waits."""
    global _DROPPED
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        else:
            queue.task_done()
            with _LOCK:
                _DROPPED += 1
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        with _LOCK:
            _DROPPED += 1
        return False
    return True


def dropped() -> int:
    with _LOCK:
        return _DROPPED


def percentile(samples: list[float], pct: float) -> float | None:
    """Linear interpolation between nearest ranks. Same rule as the acceptance script."""
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[int(rank)]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _stats(samples: list[float]) -> dict:
    return {
        "n": len(samples),
        "p50_ms": percentile(samples, 50),
        "p95_ms": percentile(samples, 95),
        "p99_ms": percentile(samples, 99),
        "min_ms": min(samples) if samples else None,
        "max_ms": max(samples) if samples else None,
    }


def _add(buckets: dict, kind: str, name: str, value) -> None:
    number = _num(value)
    if number is None:
        return
    buckets["all"][name].append(number)
    if kind in buckets and kind != "all":
        buckets[kind][name].append(number)


def summary() -> dict:
    names = _ROW_METRICS + _PIPE_METRICS
    buckets = {kind: {name: [] for name in names} for kind in ("all", "uav", "ugv")}
    with _LOCK:
        rows = list(_ROWS)
        pipes = list(_PIPE.values())
        dropped_n = _DROPPED
    for row in rows:
        kind = row.get("asset_type") or "other"
        _add(buckets, kind, "source_to_adapter", _span(row.get("source_ms"), row.get("adapter_ms")))
        _add(buckets, kind, "adapter_to_poll", _span(row.get("adapter_ms"), row.get("poll_ms")))
        _add(buckets, kind, "poll_to_frame", _span(row.get("poll_ms"), row.get("frame_ms")))
        _add(buckets, kind, "message_gap", row.get("message_gap_ms"))
    for row in pipes:
        kind = row.get("asset_type") or "other"
        _add(buckets, kind, "poll_to_nats", _span(row.get("poll_ms"), row.get("nats_ms")))
        _add(buckets, kind, "nats_to_ingest", _span(row.get("nats_ms"), row.get("ingest_ms")))
    return {
        "durable_dropped": dropped_n,
        "by_type": {
            kind: {name: _stats(values) for name, values in metrics.items()}
            for kind, metrics in buckets.items()
        },
    }
