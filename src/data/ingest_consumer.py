"""Durable ingest: telemetry into the repository and the Redis live snapshot.

Track, detection, and alert messages published by fusion are stored here too.
Fusion does not open the database.
"""

from __future__ import annotations

import json

from pydantic import ValidationError

from src.data.bus import PoisonMessage
from src.data.redis_live import RedisLiveStore
from src.data.sequence import live_telemetry_stale, remember_live_telemetry
from src.data.subjects import validation_reason
from src.data_services.repositories import get_repositories
from src.kernel.models import Alert, Detection, Telemetry, Track

_live: RedisLiveStore | None = None


def _snapshot() -> RedisLiveStore:
    global _live
    if _live is None:
        _live = RedisLiveStore()
    return _live


def _status(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


async def handle_telemetry(subject: str, data: bytes) -> None:
    try:
        sample = Telemetry.model_validate_json(data)
    except ValidationError as exc:
        raise PoisonMessage(validation_reason(exc)) from exc
    repos = get_repositories()
    repos.telemetry.insert(
        asset_id=sample.asset_id,
        timestamp=sample.timestamp,
        lat=sample.lat,
        lon=sample.lon,
        alt=sample.alt,
        heading=sample.heading,
        speed=sample.speed,
        battery=sample.battery_percent,
        status=_status(sample.status),
        mode=sample.mode,
    )
    if live_telemetry_stale(sample.asset_id, sample.seq):
        return
    _snapshot().put("platform", sample.asset_id, sample.model_dump(mode="json"))
    remember_live_telemetry(sample.asset_id, sample.seq)


async def handle_event(subject: str, data: bytes) -> None:
    try:
        body = json.loads(data.decode())
        kind = body.get("kind")
        payload = body.get("payload")
    except (UnicodeError, json.JSONDecodeError, AttributeError) as exc:
        raise PoisonMessage(str(exc)) from exc
    repos = get_repositories()
    try:
        if kind == "track":
            track = Track.model_validate(payload)
            repos.tracks.upsert(
                track_id=track.id,
                updated_at=track.last_update,
                asset_id=track.source_asset_ids[0] if track.source_asset_ids else track.source_asset,
                lat=track.lat,
                lon=track.lon,
                label=track.label,
                status=_status(track.state),
                confidence=track.confidence,
                payload=track.model_dump(mode="json"),
            )
            _snapshot().put("track", track.id, track.model_dump(mode="json"))
            return
        if kind == "detection":
            detection = Detection.model_validate(payload)
            repos.detections.insert(
                detection_id=detection.detection_id,
                timestamp=detection.timestamp,
                asset_id=detection.asset_id,
                lat=detection.lat,
                lon=detection.lon,
                label=_status(detection.label),
                confidence=detection.confidence,
                mission_id=detection.mission_id,
            )
            _snapshot().put("detection", detection.detection_id, detection.model_dump(mode="json"))
            return
    except ValidationError as exc:
        raise PoisonMessage(validation_reason(exc)) from exc
    raise PoisonMessage(f"unknown event kind {kind!r}")


async def handle_alert(subject: str, data: bytes) -> None:
    try:
        alert = Alert.model_validate_json(data)
    except ValidationError as exc:
        raise PoisonMessage(validation_reason(exc)) from exc
    get_repositories().alerts.insert(
        alert_id=alert.id,
        timestamp=alert.timestamp,
        asset_id=alert.asset_id,
        level=_status(alert.level),
        message=alert.message,
        payload=alert.model_dump(mode="json"),
    )
    _snapshot().put("alert", alert.id, alert.model_dump(mode="json"))
