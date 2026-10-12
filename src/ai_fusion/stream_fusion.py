"""Consume telemetry and publish Track, Detection, and Alert.

No database and no vehicle adapter. The ingest consumer stores what this publishes.
"""

from __future__ import annotations

from pydantic import ValidationError

from src.data.bus import PoisonMessage, get_bus
from src.data.subjects import ALERT_SUBJECT, EVENT_SUBJECT, validation_reason
from src.kernel.models import (
    Alert,
    AlertLevel,
    Detection,
    DetectionLabel,
    Telemetry,
    Track,
    TrackState,
    TrackType,
)


def fuse(sample: Telemetry) -> tuple[Track, Detection, Alert]:
    low_battery = sample.battery_percent < 20
    track = Track(
        id=f"own-{sample.asset_id}",
        label=sample.asset_id,
        track_type=TrackType.FRIENDLY,
        state=TrackState.ACTIVE,
        lat=sample.lat,
        lon=sample.lon,
        alt=sample.alt,
        heading=sample.heading,
        speed=sample.speed,
        confidence=0.99,
        source_asset_ids=[sample.asset_id],
        source_asset=sample.asset_id,
        first_seen=sample.timestamp,
        last_update=sample.timestamp,
        last_seen=sample.timestamp,
    )
    detection = Detection(
        asset_id=sample.asset_id,
        timestamp=sample.timestamp,
        lat=sample.lat,
        lon=sample.lon,
        label=DetectionLabel.VEHICLE,
        confidence=0.99,
        raw={"source": "telemetry"},
    )
    alert = Alert(
        id=f"fuse-{sample.asset_id}",
        level=AlertLevel.WARNING if low_battery else AlertLevel.INFO,
        title="Low battery" if low_battery else "Fused position",
        message=(
            f"{sample.asset_id} at {sample.battery_percent:.0f}%"
            if low_battery
            else f"{sample.asset_id} {sample.lat:.5f},{sample.lon:.5f}"
        ),
        asset_id=sample.asset_id,
        timestamp=sample.timestamp,
        source="fusion",
    )
    return track, detection, alert


async def handle_telemetry(subject: str, data: bytes) -> None:
    try:
        sample = Telemetry.model_validate_json(data)
    except ValidationError as exc:
        raise PoisonMessage(validation_reason(exc)) from exc
    track, detection, alert = fuse(sample)
    bus = get_bus()
    published = [
        await bus.publish_jetstream(
            EVENT_SUBJECT,
            {"kind": "track", "payload": track.model_dump(mode="json")},
        ),
        await bus.publish_jetstream(
            EVENT_SUBJECT,
            {"kind": "detection", "payload": detection.model_dump(mode="json")},
        ),
        await bus.publish_jetstream(ALERT_SUBJECT, alert.model_dump(mode="json")),
    ]
    if not all(published):
        raise RuntimeError("fusion publish failed")
